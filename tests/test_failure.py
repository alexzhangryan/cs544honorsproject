"""Worker failure handling on a real boss and real worker processes.

The cluster fixture uses a 1.5 second heartbeat timeout, so a dead worker
is noticed quickly.
"""

import os

from helpers import read_output, write_input

# About 0.6 seconds of work per map task with 6 map tasks: long enough that
# a worker can be killed while it is certainly in the middle of one.
LINES = [f"word{i % 7} word{i % 3} common" for i in range(360)]
SLOW_JOB = (
    "import time\n"
    "def map_fn(key, value):\n"
    "    time.sleep(0.01)\n"
    "    for word in value.split():\n"
    "        yield word, 1\n"
    "def reduce_fn(key, values):\n"
    "    yield key, sum(values)\n"
)


def expected_counts():
    counts = {}
    for line in LINES:
        for word in line.split():
            counts[word] = counts.get(word, 0) + 1
    return {word: str(count) for word, count in counts.items()}


def submit_slow_job(cluster, shared_root, tmp_path, maps=6, reduces=2):
    write_input(shared_root, {"input/lines.txt": "\n".join(LINES) + "\n"})
    job_file = tmp_path / "slow.py"
    job_file.write_text(SLOW_JOB)
    return cluster.submit(job_file, "input", "output/slow", maps=maps, reduces=reduces)


def test_kill_one_worker_mid_task(cluster, shared_root, tmp_path):
    cluster.start_workers(4)
    job_id = submit_slow_job(cluster, shared_root, tmp_path)
    cluster.wait_until_busy(0)

    cluster.kill_worker(0)
    status = cluster.wait(job_id)

    assert status.state == "SUCCEEDED", status.error
    assert status.requeues == 1
    assert read_output(shared_root, "output/slow") == expected_counts()
    # Nothing but finished part files in the output directory.
    assert sorted(os.listdir(shared_root / "output/slow")) == ["part-00000.txt", "part-00001.txt"]

    events = cluster.events(job_id)
    [lost] = [event for event in events if event["event"] == "requeued"]
    assert lost["worker"] == "w0"
    rerun = [
        event for event in events
        if event["event"] == "completed" and (event["kind"], event["task"]) == (lost["kind"], lost["task"])
    ]
    assert len(rerun) == 1
    assert rerun[0]["attempt"] == lost["attempt"] + 1
    assert rerun[0]["worker"] != "w0"
    assert not cluster.workers()["w0"].alive


def test_kill_all_but_one(cluster, shared_root, tmp_path):
    cluster.start_workers(4)
    job_id = submit_slow_job(cluster, shared_root, tmp_path)
    cluster.wait_until_busy(0, 1, 2)

    for index in (0, 1, 2):
        cluster.kill_worker(index)
    status = cluster.wait(job_id)

    assert status.state == "SUCCEEDED", status.error
    assert status.requeues == 3
    assert read_output(shared_root, "output/slow") == expected_counts()
    survivors = {event["worker"] for event in cluster.events(job_id)[-4:] if "worker" in event}
    assert survivors == {"w3"}


def test_restarted_worker_rejoins(cluster, shared_root, tmp_path):
    cluster.start_workers(2)
    job_id = submit_slow_job(cluster, shared_root, tmp_path)
    cluster.wait_until_busy(0)

    cluster.kill_worker(0)
    cluster.start_worker(0)  # comes back under the same name
    status = cluster.wait(job_id)

    assert status.state == "SUCCEEDED", status.error
    assert status.requeues == 1
    assert read_output(shared_root, "output/slow") == expected_counts()
    events = cluster.events(job_id)
    requeued_at = next(i for i, event in enumerate(events) if event["event"] == "requeued")
    finished_after_return = [
        event for event in events[requeued_at:]
        if event["event"] == "completed" and event["worker"] == "w0"
    ]
    assert finished_after_return, "the restarted worker did no more work"
    assert cluster.workers()["w0"].alive


def test_healthy_long_task_not_declared_dead(cluster, shared_root, tmp_path):
    write_input(shared_root, {"input/one.txt": "only line\n"})
    busy = tmp_path / "busy.py"
    busy.write_text(
        "import time\n"
        "def map_fn(key, value):\n"
        "    end = time.monotonic() + 3.0\n"  # twice the heartbeat timeout
        "    while time.monotonic() < end:\n"
        "        pass\n"
        "    yield value, 1\n"
        "def reduce_fn(key, values):\n"
        "    yield key, sum(values)\n"
    )
    cluster.start_workers(1)

    status = cluster.wait(cluster.submit(busy, "input", "output/busy", maps=1, reduces=1))

    assert status.state == "SUCCEEDED", status.error
    assert status.requeues == 0
    assert cluster.workers()["w0"].alive
