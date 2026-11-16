"""The boss's gRPC interface, with the test playing the part of a worker."""

import json
import re
import shutil
import threading
import time

import grpc
import pytest

from helpers import TEXTS, WORDCOUNT, as_task, read_output, reference_wordcount, write_input
from mapreduce import mapreduce_pb2 as pb
from mapreduce import mapreduce_pb2_grpc as pb_grpc
from mapreduce.boss import start_boss, watch_workers
from mapreduce.storage import events_path, intermediate_dir, job_source_path, resolve_inside
from mapreduce.tasks import run_task


@pytest.fixture
def stub(shared_root):
    handle = start_boss(shared_root, host="127.0.0.1", port=0)
    channel = grpc.insecure_channel(f"127.0.0.1:{handle.port}")
    yield pb_grpc.BossStub(channel)
    channel.close()
    handle.stop()


def wordcount_request(**overrides):
    fields = dict(
        input_path="input/books",
        output_path="output/wc",
        job_source=WORDCOUNT.read_text(),
        num_map=3,
        num_reduce=2,
    )
    fields.update(overrides)
    return pb.SubmitJobRequest(**fields)


def work_until_idle(stub, shared_root, worker_id="w1"):
    """Do what a worker does: take tasks and run them until none are left."""
    while True:
        assignment = stub.GetTask(pb.GetTaskRequest(worker_id=worker_id))
        if assignment.kind == pb.NONE:
            return
        run_task(str(shared_root), as_task(assignment, worker_id))
        reply = stub.ReportTask(
            pb.ReportTaskRequest(
                worker_id=worker_id,
                job_id=assignment.job_id,
                kind=assignment.kind,
                task_id=assignment.task_id,
                attempt=assignment.attempt,
                success=True,
            )
        )
        assert reply.accepted


def test_submit_and_run_to_success(stub, shared_root):
    write_input(shared_root, TEXTS)

    job_id = stub.SubmitJob(wordcount_request()).job_id

    assert re.fullmatch(r"job-\d{8}-\d{6}-[0-9a-f]{4}", job_id)
    assert job_source_path(shared_root, job_id).read_text() == WORDCOUNT.read_text()
    status = stub.GetJobStatus(pb.JobStatusRequest(job_id=job_id))
    assert (status.state, status.maps_total, status.reduces_total) == ("RUNNING", 3, 2)

    work_until_idle(stub, shared_root)

    status = stub.GetJobStatus(pb.JobStatusRequest(job_id=job_id))
    assert (status.state, status.maps_done, status.reduces_done) == ("SUCCEEDED", 3, 2)
    assert status.output_path == "output/wc"
    assert read_output(shared_root, "output/wc") == reference_wordcount(TEXTS)
    assert not intermediate_dir(shared_root, job_id).exists()


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"num_map": 0}, "map tasks"),
        ({"num_reduce": 0}, "reduce tasks"),
        ({"num_map": 10_001}, "map tasks"),
        ({"num_reduce": 100_000_000}, "reduce tasks"),
        ({"input_path": "/etc/passwd"}, "relative"),
        ({"input_path": "../outside"}, "inside shared storage"),
        ({"output_path": "output/../../outside"}, "inside shared storage"),
        ({"input_path": "input/nope"}, "not found"),
        ({"input_path": "input/emptydir"}, "no input files"),
        ({"output_path": "output/taken"}, "already exists"),
        ({"job_source": "def map_fn(:\n"}, "syntax error"),
        ({"job_source": "return 1\n"}, "syntax error"),
        ({"job_source": "def map_fn(key, value):\n    yield value, 1\n"}, "reduce_fn"),
        ({"job_source": "def reduce_fn(key, values):\n    yield key, 1\n"}, "map_fn"),
    ],
)
def test_submit_rejects_bad_requests(stub, shared_root, overrides, message):
    write_input(shared_root, TEXTS)
    write_input(shared_root, {"output/taken/part-00000.txt": "old\t1\n"})
    (shared_root / "input/emptydir").mkdir()

    with pytest.raises(grpc.RpcError) as error:
        stub.SubmitJob(wordcount_request(**overrides))

    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert message in error.value.details()
    assert "\n" not in error.value.details()
    assert not (shared_root / "jobs").exists()  # nothing was created for the rejected job


def test_submit_allows_existing_empty_output_dir(stub, shared_root):
    write_input(shared_root, TEXTS)
    (shared_root / "output/wc").mkdir(parents=True)

    assert stub.SubmitJob(wordcount_request()).job_id


def test_status_unknown_job_not_found(stub):
    with pytest.raises(grpc.RpcError) as error:
        stub.GetJobStatus(pb.JobStatusRequest(job_id="job-nope"))

    assert error.value.code() == grpc.StatusCode.NOT_FOUND
    assert "job-nope" in error.value.details()


def test_failed_report_fails_job(stub, shared_root):
    write_input(shared_root, TEXTS)
    job_id = stub.SubmitJob(wordcount_request()).job_id
    assignment = stub.GetTask(pb.GetTaskRequest(worker_id="w1"))

    stub.ReportTask(
        pb.ReportTaskRequest(
            worker_id="w1", job_id=job_id, kind=assignment.kind, task_id=assignment.task_id,
            attempt=assignment.attempt, success=False, error="ValueError: boom",
        )
    )

    status = stub.GetJobStatus(pb.JobStatusRequest(job_id=job_id))
    assert status.state == "FAILED"
    assert "boom" in status.error
    assert stub.GetTask(pb.GetTaskRequest(worker_id="w1")).kind == pb.NONE


def test_events_file_written(stub, shared_root):
    write_input(shared_root, TEXTS)
    job_id = stub.SubmitJob(wordcount_request()).job_id
    events_file = events_path(shared_root, job_id)
    assert [json.loads(line)["event"] for line in events_file.read_text().splitlines()] == ["submitted"]

    work_until_idle(stub, shared_root)

    events = [json.loads(line) for line in events_file.read_text().splitlines()]
    assert events[-1]["event"] == "job_succeeded"
    assert sum(event["event"] == "completed" for event in events) == 5  # 3 maps + 2 reduces


def test_resolve_inside(tmp_path):
    assert resolve_inside(tmp_path, "input/books") == tmp_path.resolve() / "input/books"
    assert resolve_inside(tmp_path, "a/../b") == tmp_path.resolve() / "b"
    for bad in ("/etc", "..", "a/../../b", "", "."):
        with pytest.raises(ValueError):
            resolve_inside(tmp_path, bad)


def test_submit_rejects_output_of_a_running_job(stub, shared_root):
    write_input(shared_root, TEXTS)
    running = stub.SubmitJob(wordcount_request()).job_id  # no workers, so it stays running

    for same_directory in ("output/wc", "output/./wc", "output/x/../wc"):
        with pytest.raises(grpc.RpcError) as error:
            stub.SubmitJob(wordcount_request(output_path=same_directory))
        assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert running in error.value.details()
        assert "\n" not in error.value.details()

    assert stub.SubmitJob(wordcount_request(output_path="output/other")).job_id


def test_boss_survives_a_job_directory_being_removed(stub, shared_root):
    write_input(shared_root, TEXTS)
    doomed = stub.SubmitJob(wordcount_request(output_path="output/doomed")).job_id
    shutil.rmtree(shared_root / "jobs")  # someone empties shared storage while the job runs

    assignment = stub.GetTask(pb.GetTaskRequest(worker_id="w1"))  # the event log cannot be written
    assert assignment.kind == pb.MAP
    stub.ReportTask(
        pb.ReportTaskRequest(
            worker_id="w1", job_id=doomed, kind=assignment.kind, task_id=assignment.task_id,
            attempt=assignment.attempt, success=False, error="the job file is gone",
        )
    )
    assert stub.GetJobStatus(pb.JobStatusRequest(job_id=doomed)).state == "FAILED"

    healthy = stub.SubmitJob(wordcount_request(output_path="output/healthy")).job_id
    work_until_idle(stub, shared_root)
    assert stub.GetJobStatus(pb.JobStatusRequest(job_id=healthy)).state == "SUCCEEDED"
    assert read_output(shared_root, "output/healthy") == reference_wordcount(TEXTS)


def test_watch_workers_keeps_going_after_an_error():
    class FlakyScheduler:
        calls = 0

        def check_workers(self):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("boom")
            return []

    scheduler = FlakyScheduler()
    stop = threading.Event()
    thread = threading.Thread(target=watch_workers, args=(scheduler, 0.01, stop), daemon=True)
    thread.start()
    deadline = time.monotonic() + 5
    while scheduler.calls < 3 and time.monotonic() < deadline:
        time.sleep(0.01)
    stop.set()
    thread.join(timeout=5)

    assert scheduler.calls >= 3, "the watcher stopped after one failed check"


def test_submit_accepts_functions_defined_by_assignment(stub, shared_root):
    write_input(shared_root, TEXTS)
    source = (
        "def count(key, values):\n"
        "    yield key, sum(values)\n"
        "def words(key, value):\n"
        "    for word in value.split():\n"
        "        yield word.lower(), 1\n"
        "map_fn = words\n"
        "reduce_fn = count\n"
    )

    job_id = stub.SubmitJob(wordcount_request(job_source=source)).job_id
    work_until_idle(stub, shared_root)

    assert stub.GetJobStatus(pb.JobStatusRequest(job_id=job_id)).state == "SUCCEEDED"
    assert read_output(shared_root, "output/wc") == reference_wordcount(TEXTS)


def test_boss_refuses_a_heartbeat_timeout_shorter_than_two_heartbeats(shared_root):
    import subprocess
    import sys

    from helpers import REPO

    result = subprocess.run(
        [sys.executable, "-m", "mapreduce.boss", "--shared", str(shared_root), "--port", "0",
         "--heartbeat-timeout", "1"],
        cwd=REPO, capture_output=True, text=True, timeout=30,
    )

    assert result.returncode != 0
    assert "--heartbeat-timeout must be at least 2" in result.stderr
    assert "Traceback" not in result.stderr


# What check_job_source accepts and rejects, without running the file.

FUNCTIONS = "def m(key, value):\n    yield value, 1\ndef r(key, values):\n    yield key, 1\n"


@pytest.mark.parametrize(
    "source",
    [
        FUNCTIONS + "map_fn, reduce_fn = m, r\n",
        "from os import path as map_fn\nimport os as reduce_fn\n",
        "import sys\nif sys.version_info > (3,):\n    def map_fn(k, v):\n        yield v, 1\n"
        "try:\n    def reduce_fn(k, vs):\n        yield k, 1\nfinally:\n    pass\n",
        "from somewhere import *\n",
        FUNCTIONS + "def install():\n    global map_fn, reduce_fn\n    map_fn, reduce_fn = m, r\ninstall()\n",
        FUNCTIONS + "globals()['map_fn'] = m\nglobals()['reduce_fn'] = r\n",
        FUNCTIONS + "exec('map_fn, reduce_fn = m, r')\n",
        FUNCTIONS + "match (m, r):\n    case (map_fn, reduce_fn):\n        pass\n",
        # Deeply nested, but fine for the compiler and for the workers.
        "def map_fn(k, v):\n    yield v, 1\ndef reduce_fn(k, vs):\n    yield k, 1\nx = "
        + " + ".join(["'a'"] * 600) + "\n",
    ],
    ids=["assignment", "imports", "inside-if-and-try", "star-import", "global-statement",
         "globals-call", "exec-call", "match-capture", "deep-nesting"],
)
def test_check_job_source_accepts(source):
    from mapreduce.boss import check_job_source

    check_job_source(source)  # must not raise


@pytest.mark.parametrize(
    "source, message",
    [
        ("def helper():\n    map_fn = 1\n    reduce_fn = 2\n", "map_fn"),
        ("class Job:\n    def map_fn(self):\n        pass\n    def reduce_fn(self):\n        pass\n", "map_fn"),
        ("def map_fn(k, v):\n    yield v, 1\nreducer = lambda k, vs: [(k, 1)]\n", "reduce_fn"),
        ("def map_fn(k, v):\n    yield v, 1\n\x00", "not valid Python"),
    ],
    ids=["only-inside-a-function", "only-inside-a-class", "wrong-name", "nul-byte"],
)
def test_check_job_source_rejects(source, message):
    from mapreduce.boss import check_job_source

    with pytest.raises(ValueError) as error:
        check_job_source(source)

    assert message in str(error.value)
    assert "None" not in str(error.value)
    assert "\n" not in str(error.value)

# Reduce order and intermediate data size.


def lopsided_input(shared_root):
    """Input where one word is far more common than the rest."""
    write_input(shared_root, {"input/books/one.txt": "common " * 500 + "\nrare other words here\n"})


def work_through_map_phase(stub, shared_root, worker_id="w1"):
    """Run map tasks until the boss hands out a reduce task, and return that."""
    while True:
        assignment = stub.GetTask(pb.GetTaskRequest(worker_id=worker_id))
        if assignment.kind == pb.REDUCE:
            return assignment
        run_task(str(shared_root), as_task(assignment, worker_id))
        stub.ReportTask(
            pb.ReportTaskRequest(
                worker_id=worker_id, job_id=assignment.job_id, kind=assignment.kind,
                task_id=assignment.task_id, attempt=assignment.attempt, success=True,
            )
        )


def test_largest_reduce_task_is_handed_out_first(stub, shared_root):
    from mapreduce.tasks import partition

    lopsided_input(shared_root)
    stub.SubmitJob(wordcount_request(num_map=2, num_reduce=5))

    first_reduce = work_through_map_phase(stub, shared_root)

    assert first_reduce.task_id == partition("common", 5)


def test_numerical_reduce_order_on_request(stub, shared_root):
    from mapreduce.tasks import partition

    lopsided_input(shared_root)
    assert partition("common", 5) != 0, "pick another word: this one lands in reduce task 0"
    stub.SubmitJob(wordcount_request(num_map=2, num_reduce=5, reduce_order="numerical"))

    assert work_through_map_phase(stub, shared_root).task_id == 0


def test_unknown_reduce_order_is_rejected(stub, shared_root):
    write_input(shared_root, TEXTS)

    with pytest.raises(grpc.RpcError) as error:
        stub.SubmitJob(wordcount_request(reduce_order="smallest_first"))

    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert "reduce order" in error.value.details()


def test_status_reports_size_of_intermediate_data(stub, shared_root):
    lopsided_input(shared_root)
    job_id = stub.SubmitJob(wordcount_request(num_map=2, num_reduce=5)).job_id
    assert stub.GetJobStatus(pb.JobStatusRequest(job_id=job_id)).intermediate_bytes == 0

    work_through_map_phase(stub, shared_root)

    on_disk = sum(f.stat().st_size for f in intermediate_dir(shared_root, job_id).glob("map-*.jsonl"))
    assert on_disk > 0
    assert stub.GetJobStatus(pb.JobStatusRequest(job_id=job_id)).intermediate_bytes == on_disk


def test_reduce_input_sizes_counts_only_finished_intermediate_files(shared_root):
    from mapreduce.boss import reduce_input_sizes

    directory = intermediate_dir(shared_root, "job-1")
    directory.mkdir(parents=True)
    files = {
        "map-0-reduce-0.jsonl": 10,
        "map-1-reduce-0.jsonl": 5,
        "map-0-reduce-1.jsonl": 7,
        "map-12-reduce-1.jsonl": 100,
        "map-0-reduce-1.jsonl.tmp-w0-1": 1000,  # half-written by a killed worker
        "reduce-0.out.tmp-w1-2": 1000,  # a reduce task's temporary output
        "map-0-reduce-7.jsonl": 1000,  # no such reduce task in this job
    }
    for name, size in files.items():
        (directory / name).write_bytes(b"x" * size)

    assert reduce_input_sizes(shared_root)("job-1", 3) == [15, 107, 0]
