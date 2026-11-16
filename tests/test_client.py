"""The command-line client, run in-process against a real cluster."""

import time

from helpers import TEXTS, WORDCOUNT, read_output, reference_wordcount, write_input
from mapreduce import mapreduce_pb2 as pb
from mapreduce.client import build_parser, main


def client(cluster, shared_root, *args):
    return main(["--boss", f"127.0.0.1:{cluster.port}", "--shared", str(shared_root), *args])


def submit_args(job=WORDCOUNT, output="output/wc"):
    return ["submit", "--input", "input/books", "--output", output, "--job", str(job),
            "--maps", "3", "--reduces", "2"]


def assert_one_line_error(captured, fragment):
    assert captured.err.startswith("error: ")
    assert captured.err.count("\n") == 1
    assert fragment in captured.err
    assert "Traceback" not in captured.err + captured.out


def test_upload_directory_and_file(shared_root, tmp_path, capsys):
    local = tmp_path / "local"
    (local / "sub").mkdir(parents=True)
    (local / "a.txt").write_text("alpha\n")
    (local / "sub" / "b.txt").write_text("beta\n")

    assert main(["--shared", str(shared_root), "upload", str(local), "input/data"]) == 0
    assert (shared_root / "input/data/a.txt").read_text() == "alpha\n"
    assert (shared_root / "input/data/sub/b.txt").read_text() == "beta\n"
    assert "input/data" in capsys.readouterr().out

    assert main(["--shared", str(shared_root), "upload", str(local / "a.txt"), "input/single.txt"]) == 0
    assert (shared_root / "input/single.txt").read_text() == "alpha\n"


def test_upload_rejects_escaping_dest(shared_root, tmp_path, capsys):
    local = tmp_path / "a.txt"
    local.write_text("alpha\n")

    assert main(["--shared", str(shared_root), "upload", str(local), "../outside.txt"]) == 1

    assert_one_line_error(capsys.readouterr(), "inside shared storage")
    assert not (shared_root.parent / "outside.txt").exists()


def test_upload_missing_source(shared_root, tmp_path, capsys):
    assert main(["--shared", str(shared_root), "upload", str(tmp_path / "nope"), "input/x"]) == 1

    assert_one_line_error(capsys.readouterr(), "nope")


def test_upload_missing_shared_dir(tmp_path, capsys):
    local = tmp_path / "a.txt"
    local.write_text("alpha\n")

    assert main(["--shared", str(tmp_path / "no-shared"), "upload", str(local), "input/a.txt"]) == 1

    assert_one_line_error(capsys.readouterr(), "shared storage directory not found")


def test_submit_waits_and_reports_success(cluster, shared_root, capsys):
    write_input(shared_root, TEXTS)
    cluster.start_workers(2)

    assert client(cluster, shared_root, *submit_args()) == 0

    out = capsys.readouterr().out
    assert "submitted job-" in out
    assert "map 3/3  reduce 2/2" in out
    assert "SUCCEEDED" in out
    assert "output/wc" in out
    assert read_output(shared_root, "output/wc") == reference_wordcount(TEXTS)


def test_submit_no_wait_returns_immediately(cluster, shared_root, capsys):
    write_input(shared_root, TEXTS)  # no workers, so the job cannot finish

    started = time.monotonic()
    assert client(cluster, shared_root, *submit_args(), "--no-wait") == 0

    assert time.monotonic() - started < 5
    job_id = capsys.readouterr().out.split()[1]
    assert cluster.status(job_id).state == "RUNNING"


def test_submit_bad_output_dir_exits_1_with_one_line_error(cluster, shared_root, capsys):
    write_input(shared_root, TEXTS)
    write_input(shared_root, {"output/wc/part-00000.txt": "old\t1\n"})

    assert client(cluster, shared_root, *submit_args()) == 1

    assert_one_line_error(capsys.readouterr(), "already exists")


def test_submit_missing_job_file(cluster, shared_root, tmp_path, capsys):
    write_input(shared_root, TEXTS)

    assert client(cluster, shared_root, *submit_args(job=tmp_path / "nope.py")) == 1

    assert_one_line_error(capsys.readouterr(), "job file not found")


def test_failed_job_exits_1_and_prints_error(cluster, shared_root, tmp_path, capsys):
    write_input(shared_root, TEXTS)
    bad = tmp_path / "bad.py"
    bad.write_text(
        "def map_fn(key, value):\n"
        "    raise ValueError('bad line')\n"
        "def reduce_fn(key, values):\n"
        "    yield key, sum(values)\n"
    )
    cluster.start_workers(1)

    assert client(cluster, shared_root, *submit_args(job=bad)) == 1

    out = capsys.readouterr().out
    assert "FAILED" in out
    assert "ValueError: bad line" in out


def test_status_command(cluster, shared_root, capsys):
    write_input(shared_root, TEXTS)
    cluster.start_workers(1)
    job_id = cluster.submit(WORDCOUNT, "input/books", "output/wc", maps=3, reduces=2)
    cluster.wait(job_id)

    assert client(cluster, shared_root, "status", job_id) == 0
    out = capsys.readouterr().out
    assert job_id in out
    assert "SUCCEEDED" in out
    assert "map 3/3  reduce 2/2" in out

    assert client(cluster, shared_root, "status", "job-nope") == 1
    assert_one_line_error(capsys.readouterr(), "unknown job")


def test_boss_unreachable_message(shared_root, capsys):
    started = time.monotonic()

    assert main(["--boss", "127.0.0.1:1", "--shared", str(shared_root), "status", "job-x"]) == 1

    assert time.monotonic() - started < 10
    assert_one_line_error(capsys.readouterr(), "cannot reach the boss at 127.0.0.1:1")


def test_workers_command(cluster, shared_root, capsys):
    cluster.start_workers(2)
    cluster.wait_until(lambda: len(cluster.workers()) == 2, "both workers have contacted the boss")

    assert client(cluster, shared_root, "workers") == 0
    lines = capsys.readouterr().out.splitlines()
    assert [line.split()[:2] for line in lines] == [["w0", "alive"], ["w1", "alive"]]

    cluster.kill_worker(0)
    cluster.wait_until(lambda: not cluster.workers()["w0"].alive, "w0 is declared dead")

    assert client(cluster, shared_root, "workers") == 0
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].split()[:2] == ["w0", "DEAD"]
    assert "last contact" in lines[0]
    assert lines[1].split()[:2] == ["w1", "alive"]


def test_workers_command_with_no_workers(cluster, shared_root, capsys):
    assert client(cluster, shared_root, "workers") == 0

    assert "no workers" in capsys.readouterr().out


# Mistakes on the command line are reported as one line, never as a traceback.


def test_job_path_that_is_a_directory(cluster, shared_root, tmp_path, capsys):
    write_input(shared_root, TEXTS)

    assert client(cluster, shared_root, *submit_args(job=tmp_path)) == 1

    assert_one_line_error(capsys.readouterr(), "cannot read the job file")


def test_job_file_that_is_not_text(cluster, shared_root, tmp_path, capsys):
    write_input(shared_root, TEXTS)
    binary = tmp_path / "job.py"
    binary.write_bytes(b"\xff\xfe\x00\x80 not text")

    assert client(cluster, shared_root, *submit_args(job=binary)) == 1

    assert_one_line_error(capsys.readouterr(), "cannot read the job file")


def test_number_of_tasks_out_of_range(cluster, shared_root, capsys):
    write_input(shared_root, TEXTS)
    args = submit_args()
    args[args.index("--maps") + 1] = "3000000000"

    assert client(cluster, shared_root, *args) == 1

    assert_one_line_error(capsys.readouterr(), "--maps")


def test_upload_that_cannot_be_copied(shared_root, tmp_path, capsys):
    local = tmp_path / "local"
    local.mkdir()
    (local / "a.txt").write_text("alpha\n")
    (shared_root / "input").write_text("a file where a directory is needed")

    assert main(["--shared", str(shared_root), "upload", str(local), "input/data"]) == 1

    assert_one_line_error(capsys.readouterr(), "cannot copy")


def test_upload_of_a_directory_that_contains_shared_storage(shared_root, tmp_path, capsys):
    (tmp_path / "notes.txt").write_text("hello\n")  # tmp_path also holds the shared directory

    assert main(["--shared", str(shared_root), "upload", str(tmp_path), "input/all"]) == 1

    assert_one_line_error(capsys.readouterr(), "into itself")
    assert not (shared_root / "input").exists()


def test_more_tasks_than_the_system_allows(cluster, shared_root, capsys):
    write_input(shared_root, TEXTS)
    args = submit_args()
    args[args.index("--reduces") + 1] = "10001"

    assert client(cluster, shared_root, *args) == 1

    assert_one_line_error(capsys.readouterr(), "--reduces must be between 1 and 10000")

def first_reduce_task_handed_out(cluster, job_id):
    return next(
        event["task"] for event in cluster.events(job_id)
        if event["event"] == "assigned" and event["kind"] == "reduce"
    )


def lopsided_submit_args(output):
    """A job whose largest reduce task is not task 0."""
    return ["submit", "--input", "input/lopsided", "--output", output, "--job", str(WORDCOUNT),
            "--maps", "2", "--reduces", "5"]


def test_reduce_order_option_reaches_the_boss(cluster, shared_root, capsys):
    from mapreduce.tasks import partition

    write_input(shared_root, {"input/lopsided/one.txt": "common " * 500 + "\nrare other words here\n"})
    largest = partition("common", 5)
    assert largest != 0, "pick another word: this one lands in reduce task 0"
    cluster.start_workers(2)

    assert client(cluster, shared_root, *lopsided_submit_args("output/default")) == 0
    default_job = capsys.readouterr().out.split()[1]
    assert client(cluster, shared_root, *lopsided_submit_args("output/numerical"), "--reduce-order", "numerical") == 0
    numerical_job = capsys.readouterr().out.split()[1]

    assert first_reduce_task_handed_out(cluster, default_job) == largest
    assert first_reduce_task_handed_out(cluster, numerical_job) == 0


def test_submit_shows_the_size_of_the_intermediate_data(cluster, shared_root, capsys):
    write_input(shared_root, TEXTS)
    cluster.start_workers(2)

    assert client(cluster, shared_root, *submit_args()) == 0

    out = capsys.readouterr().out
    job_id = out.split()[1]
    measured = cluster.status(job_id).intermediate_bytes
    assert measured > 0
    assert f"intermediate data: {measured} bytes" in out  # small enough to be shown in bytes


def test_submit_says_nothing_about_intermediate_data_that_was_not_measured(capsys):
    from mapreduce.client import cmd_submit

    class Stub:
        def SubmitJob(self, request, timeout=None):
            return type("Reply", (), {"job_id": "job-1"})()

        def GetJobStatus(self, request, timeout=None):
            return pb.JobStatusReply(job_id="job-1", state="SUCCEEDED", output_path="out")

    args = build_parser().parse_args(["submit", "--input", "in", "--output", "out", "--job", str(WORDCOUNT),
                                      "--maps", "1", "--reduces", "1"])

    assert cmd_submit(args, Stub()) == 0

    assert "intermediate data" not in capsys.readouterr().out


def test_submit_options_reach_the_boss(tmp_path):
    from mapreduce.client import submit_job

    class RecordingStub:
        def SubmitJob(self, request, timeout=None):
            self.request = request
            return type("Reply", (), {"job_id": "job-1"})()

    stub = RecordingStub()
    job = tmp_path / "job.py"
    job.write_text("def map_fn(k, v):\n    yield v, 1\ndef reduce_fn(k, vs):\n    yield k, 1\n")

    submit_job(stub, "in", "out", job, 2, 3, reduce_order="numerical", backup_tasks=False)

    assert (stub.request.num_map, stub.request.num_reduce) == (2, 3)
    assert stub.request.reduce_order == "numerical"
    assert stub.request.no_backup_tasks is True


def test_submit_command_passes_its_options_on(capsys):
    from mapreduce.client import cmd_submit

    class RecordingStub:
        def SubmitJob(self, request, timeout=None):
            self.request = request
            return type("Reply", (), {"job_id": "job-1"})()

    def submitted(*options):
        stub = RecordingStub()
        args = build_parser().parse_args(["submit", "--input", "in", "--output", "out", "--job", str(WORDCOUNT),
                                          "--maps", "2", "--reduces", "3", "--no-wait", *options])
        assert cmd_submit(args, stub) == 0
        return stub.request

    plain = submitted()
    assert (plain.no_backup_tasks, plain.reduce_order) == (False, "largest_first")
    assert submitted("--no-backup-tasks").no_backup_tasks is True
    assert submitted("--reduce-order", "numerical").reduce_order == "numerical"
