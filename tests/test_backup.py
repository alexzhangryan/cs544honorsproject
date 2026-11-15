"""Backup tasks and cancelling, on a real boss and real worker processes."""

import os

from helpers import TEXTS, WORDCOUNT, read_output, write_input

# 8 map tasks of 60 lines each. A map task takes about 0.6 seconds on a
# normal worker and 6 seconds on the slow one.
LINES = [f"word{i % 7} word{i % 3} common" for i in range(480)]
SLOW_AWARE_JOB = (
    "import os, time\n"
    "PAUSE = 0.1 if os.environ.get('TEST_SLOW_WORKER') else 0.01\n"
    "def map_fn(key, value):\n"
    "    time.sleep(PAUSE)\n"
    "    for word in value.split():\n"
    "        yield word, 1\n"
    "def reduce_fn(key, values):\n"
    "    yield key, sum(values)\n"
)
SLOW_MAP_SECONDS = 60 * 0.1


def expected_counts():
    counts = {}
    for line in LINES:
        for word in line.split():
            counts[word] = counts.get(word, 0) + 1
    return {word: str(count) for word, count in counts.items()}


def start_with_one_slow_worker(cluster):
    cluster.start_worker(0, env={"TEST_SLOW_WORKER": "1"})
    for index in (1, 2, 3):
        cluster.start_worker(index)
    cluster.wait_until(lambda: len(cluster.workers()) == 4, "all four workers have contacted the boss")


def submit(cluster, shared_root, tmp_path, **options):
    write_input(shared_root, {"input/lines.txt": "\n".join(LINES) + "\n"})
    job_file = tmp_path / "slow_aware.py"
    job_file.write_text(SLOW_AWARE_JOB)
    return cluster.submit(job_file, "input", "output/words", maps=8, reduces=2, **options)


def test_backup_task_rescues_a_job_from_a_slow_worker(cluster, shared_root, tmp_path):
    start_with_one_slow_worker(cluster)

    status = cluster.wait(submit(cluster, shared_root, tmp_path))

    assert status.state == "SUCCEEDED", status.error
    assert read_output(shared_root, "output/words") == expected_counts()
    assert sorted(os.listdir(shared_root / "output/words")) == ["part-00000.txt", "part-00001.txt"]
    assert status.backup_tasks >= 1
    assert status.requeues == 0
    # The job did not wait for the slow worker to finish its map task.
    assert status.elapsed_seconds < SLOW_MAP_SECONDS - 1
    events = cluster.events(status.job_id)
    backups = [event for event in events if event.get("backup")]
    assert all(event["worker"] != "w0" for event in backups)
    cancelled = [event for event in events if event["event"] == "cancelled"]
    assert cancelled and all(event["worker"] == "w0" for event in cancelled)
    # The slow worker is told to stop with its next heartbeat, and does.
    cluster.wait_until(
        lambda: "no longer needed" in (cluster.log_dir / "w0.log").read_text(),
        "the slow worker has stopped its copy of the task",
    )


def test_without_backup_tasks_the_job_waits_for_the_slow_worker(cluster, shared_root, tmp_path):
    start_with_one_slow_worker(cluster)

    status = cluster.wait(submit(cluster, shared_root, tmp_path, backup_tasks=False))

    assert status.state == "SUCCEEDED", status.error
    assert read_output(shared_root, "output/words") == expected_counts()
    assert status.backup_tasks == 0
    assert status.elapsed_seconds >= SLOW_MAP_SECONDS


def test_killed_workers_task_is_left_to_the_heartbeat_timeout(cluster, shared_root, tmp_path):
    cluster.start_workers(4)
    cluster.wait_until(lambda: len(cluster.workers()) == 4, "all four workers have contacted the boss")
    job_id = submit(cluster, shared_root, tmp_path)  # backup tasks are on
    cluster.wait_until_busy(0)

    cluster.kill_worker(0)
    status = cluster.wait(job_id)

    assert status.state == "SUCCEEDED", status.error
    assert read_output(shared_root, "output/words") == expected_counts()
    # A silent worker is treated as dead, not slow: no backup is started for
    # its task. The task comes back through the timeout, as it always did.
    assert status.requeues == 1
    assert status.backup_tasks == 0


def test_failed_job_stops_tasks_still_running_on_other_workers(cluster, shared_root, tmp_path):
    # Two map tasks: one fails at once, the other would run for 30 seconds.
    write_input(shared_root, {"input/doomed/bad.txt": "boom\n", "input/doomed/slow.txt": "wait\n"})
    write_input(shared_root, {f"input/next/{i}.txt": "a b c\n" for i in range(6)})
    doomed = tmp_path / "doomed.py"
    doomed.write_text(
        "import time\n"
        "def map_fn(key, value):\n"
        "    if value == 'boom':\n"
        "        raise ValueError('boom')\n"
        "    time.sleep(30)\n"
        "    yield value, 1\n"
        "def reduce_fn(key, values):\n"
        "    yield key, sum(values)\n"
    )
    paced = tmp_path / "paced.py"
    paced.write_text(
        "import time\n"
        "def map_fn(key, value):\n"
        "    time.sleep(0.3)\n"
        "    yield value, 1\n"
        "def reduce_fn(key, values):\n"
        "    yield key, sum(values)\n"
    )
    cluster.start_workers(2)
    cluster.wait_until(lambda: len(cluster.workers()) == 2, "both workers have contacted the boss")

    failed = cluster.wait(cluster.submit(doomed, "input/doomed", "output/doomed", maps=2, reduces=1))
    assert failed.state == "FAILED"

    # If the 30-second task were still running, its worker could take no part in the next job.
    status = cluster.wait(cluster.submit(paced, "input/next", "output/next", maps=6, reduces=1), timeout=20)
    assert status.state == "SUCCEEDED", status.error
    took_part = {event["worker"] for event in cluster.events(status.job_id) if event["event"] == "completed"}
    assert took_part == {"w0", "w1"}
