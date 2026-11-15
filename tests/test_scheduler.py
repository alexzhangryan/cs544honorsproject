"""The boss's scheduling logic, driven directly with a fake clock."""

from mapreduce.scheduler import Scheduler
from mapreduce.splits import Chunk


class FakeClock:
    def __init__(self):
        self.now = 100.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def splits(num_map):
    return [[Chunk("in.txt", i * 10, (i + 1) * 10)] for i in range(num_map)]


def make(num_map=2, num_reduce=2, **kwargs):
    clock = FakeClock()
    scheduler = Scheduler(clock=clock, **kwargs)
    scheduler.add_job("job-1", "in", "out", splits(num_map), num_reduce)
    return scheduler, clock


def finish(scheduler, worker, assignment):
    return scheduler.report(
        worker, assignment.job_id, assignment.kind, assignment.task_id, assignment.attempt, True
    )


def run_to_completion(scheduler, worker="w1"):
    while True:
        assignment = scheduler.get_task(worker)
        if assignment is None:
            return
        assert finish(scheduler, worker, assignment)


def test_none_when_nothing_to_do():
    assert Scheduler().get_task("w1") is None


def test_maps_assigned_before_reduces():
    scheduler, _ = make(num_map=2, num_reduce=2)

    first = scheduler.get_task("w1")
    second = scheduler.get_task("w2")

    assert (first.kind, first.task_id, first.attempt) == ("map", 0, 1)
    assert (second.kind, second.task_id, second.attempt) == ("map", 1, 1)
    assert first.chunks == [Chunk("in.txt", 0, 10)]
    assert (first.job_id, first.num_map, first.num_reduce) == ("job-1", 2, 2)


def test_reduces_wait_for_all_maps():
    scheduler, _ = make(num_map=2, num_reduce=2)
    map0 = scheduler.get_task("w1")
    map1 = scheduler.get_task("w2")

    finish(scheduler, "w1", map0)
    assert scheduler.get_task("w1") is None  # map 1 is still running

    finish(scheduler, "w2", map1)
    reduce0 = scheduler.get_task("w1")
    assert (reduce0.kind, reduce0.task_id, reduce0.output_path) == ("reduce", 0, "out")


def test_job_succeeds_when_all_reduces_complete():
    scheduler, _ = make(num_map=2, num_reduce=2)

    run_to_completion(scheduler)

    status = scheduler.job_status("job-1")
    assert status.state == "SUCCEEDED"
    assert (status.maps_done, status.reduces_done) == (2, 2)
    assert scheduler.get_task("w1") is None


def test_task_error_fails_job_and_stops_assignment():
    scheduler, _ = make(num_map=2, num_reduce=1)
    map0 = scheduler.get_task("w1")

    accepted = scheduler.report("w1", "job-1", "map", map0.task_id, map0.attempt, False, "ValueError: boom")

    assert accepted
    status = scheduler.job_status("job-1")
    assert status.state == "FAILED"
    assert "boom" in status.error
    assert scheduler.get_task("w2") is None  # map 1 is idle but the job is dead


def test_jobs_served_in_submission_order():
    scheduler, _ = make(num_map=1, num_reduce=1)
    scheduler.add_job("job-2", "in2", "out2", splits(1), 1)

    older_map = scheduler.get_task("w1")
    newer_map = scheduler.get_task("w2")  # job-1 has nothing ready, so job-2 starts
    assert (older_map.job_id, newer_map.job_id) == ("job-1", "job-2")

    finish(scheduler, "w1", older_map)
    finish(scheduler, "w2", newer_map)
    assert scheduler.get_task("w2").job_id == "job-1"  # the older job goes first
    assert scheduler.get_task("w1").job_id == "job-2"


def test_stale_report_ignored():
    scheduler, _ = make(num_map=1, num_reduce=1)
    map0 = scheduler.get_task("w1")

    assert not scheduler.report("w1", "job-1", "map", 0, map0.attempt + 1, True)  # wrong attempt
    assert not scheduler.report("w2", "job-1", "map", 0, map0.attempt, True)  # wrong worker
    assert not scheduler.report("w1", "job-x", "map", 0, map0.attempt, True)  # unknown job
    assert not scheduler.report("w1", "job-1", "map", 9, map0.attempt, True)  # unknown task
    assert not scheduler.report("w1", "job-1", "reduce", 0, 1, True)  # not handed out yet
    assert scheduler.job_status("job-1").maps_done == 0

    assert finish(scheduler, "w1", map0)
    assert not finish(scheduler, "w1", map0)  # reported twice
    assert scheduler.job_status("job-1").maps_done == 1


def test_status_counts_and_elapsed():
    scheduler, clock = make(num_map=3, num_reduce=2)
    assert scheduler.job_status("nope") is None

    finish(scheduler, "w1", scheduler.get_task("w1"))
    clock.advance(3)
    status = scheduler.job_status("job-1")
    assert (status.state, status.maps_done, status.maps_total) == ("RUNNING", 1, 3)
    assert (status.reduces_done, status.reduces_total) == (0, 2)
    assert (status.elapsed, status.requeues, status.output_path, status.error) == (3.0, 0, "out", "")

    run_to_completion(scheduler)
    clock.advance(50)
    assert scheduler.job_status("job-1").elapsed == 3.0  # stops when the job ends


def test_events_recorded():
    seen = []
    scheduler, clock = make(num_map=1, num_reduce=1, on_event=lambda job_id, event: seen.append((job_id, event)))
    clock.advance(2)

    run_to_completion(scheduler)

    events = scheduler.events("job-1")
    assert [event["event"] for event in events] == [
        "submitted",
        "assigned",
        "completed",
        "assigned",
        "completed",
        "job_succeeded",
    ]
    assert events[1] == {"t": 2.0, "event": "assigned", "kind": "map", "task": 0, "attempt": 1, "worker": "w1"}
    assert seen == [("job-1", event) for event in events]


# Worker failure handling.


def test_silent_worker_declared_dead_after_timeout():
    scheduler, clock = make()
    scheduler.heartbeat("w1")

    clock.advance(5.0)
    assert scheduler.check_workers() == []  # exactly at the limit is still alive

    clock.advance(0.1)
    assert scheduler.check_workers() == ["w1"]
    assert scheduler.check_workers() == []  # reported only once
    assert [(w.worker_id, w.alive) for w in scheduler.workers()] == [("w1", False)]


def test_heartbeat_keeps_worker_alive():
    scheduler, clock = make()
    map0 = scheduler.get_task("w1")

    for _ in range(10):
        clock.advance(4)
        scheduler.heartbeat("w1")
        assert scheduler.check_workers() == []

    assert finish(scheduler, "w1", map0)
    assert scheduler.job_status("job-1").requeues == 0


def test_dead_workers_task_requeued_with_new_attempt():
    scheduler, clock = make()
    lost = scheduler.get_task("w1")
    assert (lost.kind, lost.task_id, lost.attempt) == ("map", 0, 1)

    clock.advance(6)
    scheduler.check_workers()

    assert scheduler.job_status("job-1").requeues == 1
    again = scheduler.get_task("w2")
    assert (again.kind, again.task_id, again.attempt) == ("map", 0, 2)


def test_completed_tasks_not_rerun():
    scheduler, clock = make(num_map=2, num_reduce=1)
    finish(scheduler, "w1", scheduler.get_task("w1"))  # map 0 is complete
    scheduler.get_task("w1")  # w1 starts map 1, then dies

    clock.advance(6)
    scheduler.check_workers()

    status = scheduler.job_status("job-1")
    assert (status.maps_done, status.requeues) == (1, 1)
    again = scheduler.get_task("w2")
    assert (again.kind, again.task_id, again.attempt) == ("map", 1, 2)


def test_late_report_from_dead_worker_ignored():
    scheduler, clock = make(num_map=1, num_reduce=1)
    first = scheduler.get_task("w1")
    clock.advance(6)
    scheduler.check_workers()

    assert not finish(scheduler, "w1", first)  # arrives while the task waits to be re-run
    second = scheduler.get_task("w2")
    assert not finish(scheduler, "w1", first)  # arrives while w2 is running the task
    assert scheduler.job_status("job-1").maps_done == 0

    assert finish(scheduler, "w2", second)
    assert scheduler.job_status("job-1").maps_done == 1


def test_dead_worker_rejoins():
    scheduler, clock = make()
    scheduler.heartbeat("w1")
    clock.advance(6)
    assert scheduler.check_workers() == ["w1"]

    assignment = scheduler.get_task("w1")

    assert assignment is not None
    assert [(w.worker_id, w.alive) for w in scheduler.workers()] == [("w1", True)]
    assert finish(scheduler, "w1", assignment)


def test_worker_asking_while_holding_task_requeues_it():
    scheduler, _ = make()
    first = scheduler.get_task("w1")

    second = scheduler.get_task("w1")  # w1 restarted and lost what it was doing

    assert (second.task_id, second.attempt) == (first.task_id, 2)
    assert scheduler.job_status("job-1").requeues == 1
    assert not finish(scheduler, "w1", first)
    assert finish(scheduler, "w1", second)


def test_three_losses_fail_job():
    scheduler, clock = make()

    for worker in ("w1", "w2"):
        scheduler.get_task(worker)
        clock.advance(6)
        scheduler.check_workers()
    assert scheduler.job_status("job-1").state == "RUNNING"

    scheduler.get_task("w3")
    clock.advance(6)
    scheduler.check_workers()

    status = scheduler.job_status("job-1")
    assert status.state == "FAILED"
    assert "map task 0 lost its worker 3 times" in status.error
    assert scheduler.get_task("w4") is None


def test_events_record_requeue():
    scheduler, clock = make()
    scheduler.get_task("w1")
    clock.advance(6)
    scheduler.check_workers()

    assert scheduler.events("job-1")[-1] == {
        "t": 6.0, "event": "requeued", "kind": "map", "task": 0, "attempt": 1, "worker": "w1",
    }


def test_dead_worker_of_failed_job_changes_nothing():
    scheduler, clock = make(num_map=2, num_reduce=1)
    scheduler.get_task("w1")
    failing = scheduler.get_task("w2")
    scheduler.report("w2", "job-1", "map", failing.task_id, failing.attempt, False, "boom")

    clock.advance(6)
    scheduler.check_workers()

    status = scheduler.job_status("job-1")
    assert (status.state, status.requeues, status.error) == ("FAILED", 0, "boom")


def test_workers_listing():
    scheduler, clock = make()
    scheduler.get_task("w2")
    scheduler.heartbeat("w1")
    clock.advance(2)
    scheduler.heartbeat("w1")

    listing = [(w.worker_id, w.alive, w.current_task, w.seconds_since_contact) for w in scheduler.workers()]

    assert listing == [("w1", True, "", 0.0), ("w2", True, "map 0 of job-1", 2.0)]
