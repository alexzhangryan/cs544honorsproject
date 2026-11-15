"""Whole jobs on a real boss and real worker processes."""

import os
import socket
import time

from cluster import Cluster
from helpers import TEXTS, WORDCOUNT, read_output, reference_wordcount, write_input


def write_job(tmp_path, name: str, source: str):
    path = tmp_path / name
    path.write_text(source)
    return path


def test_wordcount_four_workers(cluster, shared_root):
    write_input(shared_root, TEXTS)
    cluster.start_workers(4)

    job_id = cluster.submit(WORDCOUNT, "input/books", "output/wc", maps=5, reduces=3)
    status = cluster.wait(job_id)

    assert status.state == "SUCCEEDED", status.error
    assert (status.maps_done, status.reduces_done) == (5, 3)
    assert read_output(shared_root, "output/wc") == reference_wordcount(TEXTS)
    assert sorted(os.listdir(shared_root / "output/wc")) == [f"part-0000{r}.txt" for r in range(3)]


def test_tasks_are_spread_across_workers(cluster, shared_root, tmp_path):
    write_input(shared_root, {f"input/{i}.txt": f"line {i}\n" for i in range(4)})
    slow = write_job(
        tmp_path,
        "slow.py",
        "import time\n"
        "def map_fn(key, value):\n"
        "    time.sleep(1.0)\n"
        "    yield value, 1\n"
        "def reduce_fn(key, values):\n"
        "    yield key, sum(values)\n",
    )
    cluster.start_workers(4)

    job_id = cluster.submit(slow, "input", "output/slow", maps=4, reduces=1)
    status = cluster.wait(job_id)

    assert status.state == "SUCCEEDED", status.error
    map_workers = {
        event["worker"]
        for event in cluster.events(job_id)
        if event["event"] == "completed" and event["kind"] == "map"
    }
    assert map_workers == {"w0", "w1", "w2", "w3"}


def test_tiny_input_many_tasks(cluster, shared_root):
    texts = {"input/tiny.txt": "b a\na\nc\n"}
    write_input(shared_root, texts)
    cluster.start_workers(2)

    job_id = cluster.submit(WORDCOUNT, "input", "output/tiny", maps=8, reduces=6)
    status = cluster.wait(job_id)

    assert status.state == "SUCCEEDED", status.error
    assert read_output(shared_root, "output/tiny") == {"a": "2", "b": "1", "c": "1"}
    assert len(os.listdir(shared_root / "output/tiny")) == 6


def test_user_exception_fails_job_and_workers_survive(cluster, shared_root, tmp_path):
    write_input(shared_root, TEXTS)
    bad = write_job(
        tmp_path,
        "bad.py",
        "def map_fn(key, value):\n"
        "    raise ValueError('bad line: ' + value)\n"
        "def reduce_fn(key, values):\n"
        "    yield key, sum(values)\n",
    )
    cluster.start_workers(2)

    failed = cluster.wait(cluster.submit(bad, "input/books", "output/bad", maps=2, reduces=1))

    assert failed.state == "FAILED"
    assert "ValueError: bad line: " in failed.error
    assert "job.py" in failed.error  # the traceback points into the user's file

    good = cluster.wait(cluster.submit(WORDCOUNT, "input/books", "output/good", maps=2, reduces=1))
    assert good.state == "SUCCEEDED", good.error


def test_missing_reduce_fn_fails_job(cluster, shared_root, tmp_path):
    write_input(shared_root, TEXTS)
    only_map = write_job(tmp_path, "only_map.py", "def map_fn(key, value):\n    yield value, 1\n")
    cluster.start_workers(1)

    status = cluster.wait(cluster.submit(only_map, "input/books", "output/x", maps=1, reduces=1))

    assert status.state == "FAILED"
    assert "reduce_fn" in status.error


def test_task_process_dying_is_reported(cluster, shared_root, tmp_path):
    write_input(shared_root, TEXTS)
    dies = write_job(
        tmp_path,
        "dies.py",
        "import os\n"
        "def map_fn(key, value):\n"
        "    os._exit(3)\n"
        "def reduce_fn(key, values):\n"
        "    yield key, sum(values)\n",
    )
    cluster.start_workers(1)

    failed = cluster.wait(cluster.submit(dies, "input/books", "output/x", maps=1, reduces=1))

    assert failed.state == "FAILED"
    assert "exit code 3" in failed.error

    good = cluster.wait(cluster.submit(WORDCOUNT, "input/books", "output/good", maps=1, reduces=1))
    assert good.state == "SUCCEEDED", good.error  # the worker itself survived


def test_worker_started_before_boss(shared_root, tmp_path):
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        free_port = probe.getsockname()[1]
    write_input(shared_root, TEXTS)
    cluster = Cluster(shared_root, tmp_path / "logs", port=free_port)
    try:
        cluster.start_worker(0)
        time.sleep(2.5)  # the worker finds no boss and keeps retrying
        cluster.start_boss()

        status = cluster.wait(cluster.submit(WORDCOUNT, "input/books", "output/wc", maps=2, reduces=2), timeout=30)

        assert status.state == "SUCCEEDED", status.error
    finally:
        cluster.stop()
