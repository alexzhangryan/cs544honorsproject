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
        "map_phase_done",
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


# Robustness.


def test_failing_event_hook_does_not_corrupt_state():
    def broken_hook(job_id, event):
        raise OSError("disk full")

    scheduler, clock = make(num_map=1, num_reduce=1, on_event=broken_hook)
    lost = scheduler.get_task("w1")
    clock.advance(6)

    assert scheduler.check_workers() == ["w1"]  # the lost task is re-queued despite the hook
    again = scheduler.get_task("w2")
    assert (again.task_id, again.attempt) == (lost.task_id, 2)
    assert finish(scheduler, "w2", again)
    run_to_completion(scheduler, "w2")

    assert scheduler.job_status("job-1").state == "SUCCEEDED"
    assert scheduler.events("job-1")[-1]["event"] == "job_succeeded"  # still recorded in memory


def test_stale_report_does_not_disown_the_workers_current_task():
    scheduler, clock = make(num_map=1, num_reduce=1)
    first = scheduler.get_task("w1")
    second = scheduler.get_task("w1")  # w1's report was lost; it asks again and gets attempt 2
    assert (second.task_id, second.attempt) == (first.task_id, 2)

    assert not finish(scheduler, "w1", first)  # the delayed report for attempt 1 arrives now

    # w1 is still responsible for attempt 2, so if it dies the task must come back.
    clock.advance(6)
    assert scheduler.check_workers() == ["w1"]
    third = scheduler.get_task("w2")
    assert third is not None, "the task was orphaned: nobody will ever run it"
    assert (third.task_id, third.attempt) == (first.task_id, 3)


def test_every_job_ending_is_logged(caplog):
    caplog.set_level("INFO", logger="boss")
    scheduler, clock = make(num_map=1, num_reduce=1)
    run_to_completion(scheduler)
    assert "job job-1 SUCCEEDED" in caplog.text

    scheduler.add_job("job-2", "in", "out2", splits(1), 1)
    for worker in ("w1", "w2", "w3"):
        scheduler.get_task(worker)
        clock.advance(6)
        scheduler.check_workers()
    assert "job job-2 FAILED" in caplog.text  # failed by the three-loss limit, with no report involved


# Reduce order.


def finish_maps(scheduler, worker="w1"):
    """Complete every map task; return the first reduce assignment."""
    while True:
        assignment = scheduler.get_task(worker)
        if assignment.kind == "reduce":
            return assignment
        assert finish(scheduler, worker, assignment)


def reduce_order(scheduler, first, worker="w1"):
    """Complete reduce tasks one at a time and return the order they came in."""
    order = [first.task_id]
    assert finish(scheduler, worker, first)
    while (assignment := scheduler.get_task(worker)) is not None:
        order.append(assignment.task_id)
        assert finish(scheduler, worker, assignment)
    return order


def test_reduce_tasks_are_handed_out_largest_first():
    measured = []

    def sizes(job_id, num_reduce):
        measured.append((job_id, num_reduce))
        return [5, 50, 20, 50]

    scheduler, _ = make(num_map=2, num_reduce=4, reduce_sizes=sizes)

    first = finish_maps(scheduler)

    assert reduce_order(scheduler, first) == [1, 3, 2, 0]  # equal sizes keep their numerical order
    assert measured == [("job-1", 4)]  # measured once, when the map phase ended
    assert scheduler.job_status("job-1").intermediate_bytes == 125


def test_numerical_reduce_order_can_be_asked_for():
    clock = FakeClock()
    scheduler = Scheduler(clock=clock, reduce_sizes=lambda job_id, num_reduce: [5, 50, 20])
    scheduler.add_job("job-1", "in", "out", splits(1), 3, reduce_order="numerical")

    first = finish_maps(scheduler)

    assert reduce_order(scheduler, first) == [0, 1, 2]
    assert scheduler.job_status("job-1").intermediate_bytes == 75  # still measured


def test_failed_size_measurement_falls_back_to_numerical_order():
    def broken(job_id, num_reduce):
        raise OSError("cannot list the directory")

    scheduler, _ = make(num_map=1, num_reduce=3, reduce_sizes=broken)

    first = finish_maps(scheduler)

    assert reduce_order(scheduler, first) == [0, 1, 2]
    assert scheduler.job_status("job-1").state == "SUCCEEDED"


def test_lost_large_reduce_task_goes_back_to_the_front():
    scheduler, clock = make(num_map=1, num_reduce=3, reduce_sizes=lambda job_id, num_reduce: [5, 50, 20])
    big = finish_maps(scheduler, "w1")  # w1 now holds reduce 1, the largest
    assert big.task_id == 1

    clock.advance(6)
    scheduler.check_workers()  # w1 is dead; reduce 1 is back in the queue

    assert scheduler.get_task("w2").task_id == 1  # before the smaller ones that were never started


# Backup tasks for stragglers, and cancelling.


def wait(scheduler, clock, seconds, alive=()):
    """Let time pass in steps of half a second, with the given workers
    sending heartbeats all the while."""
    remaining = seconds
    while remaining > 1e-9:
        step = min(0.5, remaining)
        clock.advance(step)
        remaining -= step
        for worker in alive:
            scheduler.heartbeat(worker)


def four_maps_one_slow(**kwargs):
    """Four equal map tasks on w1..w4. w1, w2 and w3 finish after 2 seconds;
    w4 is alive but still running map 3. Returns (scheduler, clock, w4's assignment)."""
    scheduler, clock = make(num_map=4, num_reduce=1, **kwargs)
    assignments = {worker: scheduler.get_task(worker) for worker in ("w1", "w2", "w3", "w4")}
    wait(scheduler, clock, 2, alive=["w4"])
    for worker in ("w1", "w2", "w3"):
        assert finish(scheduler, worker, assignments[worker])
    return scheduler, clock, assignments["w4"]


def test_slow_task_gets_a_backup_on_a_free_worker():
    scheduler, clock, slow = four_maps_one_slow()

    wait(scheduler, clock, 0.5, alive=["w4"])  # map 3 has run 2.5 s; its peers took 2 s
    assert scheduler.get_task("w1") is None  # not slow enough yet (limit is 1.5 times)

    wait(scheduler, clock, 1, alive=["w4"])  # 3.5 s
    backup = scheduler.get_task("w1")

    assert (backup.kind, backup.task_id, backup.attempt, backup.backup) == ("map", slow.task_id, 2, True)
    assert not slow.backup
    assert scheduler.events("job-1")[-1] == {
        "t": 3.5, "event": "assigned", "kind": "map", "task": 3, "attempt": 2, "worker": "w1", "backup": True,
    }
    assert scheduler.job_status("job-1").backups == 1


def test_no_backup_for_a_task_whose_worker_has_gone_silent():
    scheduler, clock, slow = four_maps_one_slow()

    clock.advance(2.5)  # map 3 is very late, but w4 has not been heard from for 2.5 s

    # w4 is probably dead. Its task is left to the heartbeat timeout.
    assert scheduler.get_task("w1") is None
    clock.advance(3)
    assert scheduler.check_workers() == ["w2", "w3", "w4"]
    again = scheduler.get_task("w1")
    assert (again.task_id, again.attempt, again.backup) == (3, 2, False)
    assert scheduler.job_status("job-1").requeues == 1


def test_a_task_gets_at_most_one_backup():
    scheduler, clock, _ = four_maps_one_slow()
    wait(scheduler, clock, 1.5, alive=["w4"])
    assert scheduler.get_task("w1").backup

    wait(scheduler, clock, 10, alive=["w1", "w4"])

    assert scheduler.get_task("w2") is None


def test_no_backup_while_an_idle_task_exists():
    scheduler, clock = make(num_map=5, num_reduce=1)
    assignments = {worker: scheduler.get_task(worker) for worker in ("w1", "w2", "w3", "w4")}
    wait(scheduler, clock, 2, alive=["w4"])
    for worker in ("w1", "w2", "w3"):
        finish(scheduler, worker, assignments[worker])
    wait(scheduler, clock, 10, alive=["w4"])  # map 3 on w4 is very late by now

    fresh = scheduler.get_task("w1")

    assert (fresh.task_id, fresh.backup) == (4, False)  # real work comes first


def test_no_backup_without_enough_finished_tasks_to_compare_with():
    scheduler, clock = make(num_map=3, num_reduce=1)
    assignments = {worker: scheduler.get_task(worker) for worker in ("w1", "w2", "w3")}
    wait(scheduler, clock, 2, alive=["w3"])
    finish(scheduler, "w1", assignments["w1"])
    finish(scheduler, "w2", assignments["w2"])  # only two have finished

    wait(scheduler, clock, 100, alive=["w3"])

    assert scheduler.get_task("w1") is None


def test_no_backup_for_a_task_that_is_less_than_a_second_late():
    scheduler, clock = make(num_map=4, num_reduce=1)
    assignments = {worker: scheduler.get_task(worker) for worker in ("w1", "w2", "w3", "w4")}
    clock.advance(0.1)
    for worker in ("w1", "w2", "w3"):
        finish(scheduler, worker, assignments[worker])
    scheduler.heartbeat("w4")

    wait(scheduler, clock, 0.5, alive=["w4"])  # six times slower than its peers, but only 0.5 s late
    assert scheduler.get_task("w1") is None

    wait(scheduler, clock, 0.7, alive=["w4"])  # now 1.2 s late
    assert scheduler.get_task("w1").backup


def test_tasks_without_input_are_left_out_of_the_comparison():
    clock = FakeClock()
    scheduler = Scheduler(clock=clock)
    empty = []
    full = [Chunk("in.txt", 0, 100)]
    scheduler.add_job("job-1", "in", "out", [empty, empty, empty, full, full], 1)
    assignments = {worker: scheduler.get_task(worker) for worker in ("w1", "w2", "w3", "w4", "w5")}
    clock.advance(0.01)
    for worker in ("w1", "w2", "w3"):
        finish(scheduler, worker, assignments[worker])  # three empty tasks, done at once

    wait(scheduler, clock, 30, alive=["w4", "w5"])

    # The empty tasks say nothing about how long 100 bytes should take.
    assert scheduler.get_task("w1") is None


def test_big_task_is_not_mistaken_for_a_straggler():
    scheduler, clock = make(num_map=1, num_reduce=4, reduce_sizes=lambda job_id, n: [10, 10, 10, 100])
    big = finish_maps(scheduler, "w1")  # w1 gets reduce 3, ten times the size of the others
    assert big.task_id == 3
    small = {worker: scheduler.get_task(worker) for worker in ("w2", "w3", "w4")}
    wait(scheduler, clock, 1, alive=["w1"])
    for worker, assignment in small.items():
        finish(scheduler, worker, assignment)  # 1 s for 10 bytes, so 10 s expected for 100

    wait(scheduler, clock, 9, alive=["w1"])  # 10 s: ten times longer than the others, but on schedule
    assert scheduler.get_task("w2") is None

    wait(scheduler, clock, 2, alive=["w1"])  # 12 s: 2 s late, but only 1.2 times what its size explains
    assert scheduler.get_task("w2") is None

    wait(scheduler, clock, 4, alive=["w1"])  # 16 s: more than 1.5 times what its size explains
    assert scheduler.get_task("w2").backup


def test_backups_can_be_switched_off_for_a_job():
    clock = FakeClock()
    scheduler = Scheduler(clock=clock)
    scheduler.add_job("job-1", "in", "out", splits(4), 1, backup_tasks=False)
    assignments = {worker: scheduler.get_task(worker) for worker in ("w1", "w2", "w3", "w4")}
    wait(scheduler, clock, 2, alive=["w4"])
    for worker in ("w1", "w2", "w3"):
        finish(scheduler, worker, assignments[worker])

    wait(scheduler, clock, 100, alive=["w4"])

    assert scheduler.get_task("w1") is None


def slow_task_with_backup():
    """Map 3 is running on w4 (attempt 1) and, as a backup, on w1 (attempt 2)."""
    scheduler, clock, slow = four_maps_one_slow()
    wait(scheduler, clock, 1.5, alive=["w4"])
    backup = scheduler.get_task("w1")
    assert backup.backup
    return scheduler, clock, slow, backup


def test_backup_finishes_first():
    scheduler, clock, slow, backup = slow_task_with_backup()
    wait(scheduler, clock, 2, alive=["w1", "w4"])

    assert finish(scheduler, "w1", backup)
    assert not finish(scheduler, "w4", slow)  # the original arrives too late

    assert scheduler.job_status("job-1").maps_done == 4
    cancelled = [event for event in scheduler.events("job-1") if event["event"] == "cancelled"]
    assert [(event["task"], event["attempt"], event["worker"]) for event in cancelled] == [(3, 1, "w4")]


def test_original_finishes_first():
    scheduler, clock, slow, backup = slow_task_with_backup()

    assert finish(scheduler, "w4", slow)
    assert not finish(scheduler, "w1", backup)

    assert scheduler.job_status("job-1").maps_done == 4


def test_loser_of_the_race_is_told_to_stop():
    scheduler, clock, slow, backup = slow_task_with_backup()
    assert scheduler.heartbeat("w4") is False  # both copies are still wanted
    assert scheduler.heartbeat("w1") is False

    finish(scheduler, "w1", backup)

    assert scheduler.heartbeat("w4") is True  # stop: the other copy has finished
    assert scheduler.heartbeat("w4") is True  # and again, in case the first reply was lost
    assert scheduler.get_task("w4").kind == "reduce"  # w4 stops and carries on with new work
    assert scheduler.heartbeat("w4") is False  # the new task is wanted
    assert scheduler.job_status("job-1").requeues == 0


def test_failure_of_one_copy_leaves_the_job_to_the_other():
    scheduler, clock, slow, backup = slow_task_with_backup()

    # The copy on the struggling worker dies. The backup is still running.
    assert scheduler.report("w4", "job-1", "map", slow.task_id, slow.attempt, False, "killed: out of memory")

    assert scheduler.job_status("job-1").state == "RUNNING"
    assert scheduler.heartbeat("w1") is False  # the surviving copy is not cancelled
    assert finish(scheduler, "w1", backup)
    assert scheduler.job_status("job-1").maps_done == 4


def test_failure_of_both_copies_fails_the_job():
    scheduler, clock, slow, backup = slow_task_with_backup()

    scheduler.report("w4", "job-1", "map", slow.task_id, slow.attempt, False, "ValueError: boom")
    scheduler.report("w1", "job-1", "map", backup.task_id, backup.attempt, False, "ValueError: boom")

    status = scheduler.job_status("job-1")
    assert (status.state, status.error) == ("FAILED", "ValueError: boom")


def test_worker_dying_while_another_copy_runs_requeues_nothing():
    scheduler, clock, slow, backup = slow_task_with_backup()

    wait(scheduler, clock, 6, alive=["w1"])  # w4 goes silent; w1 keeps sending heartbeats
    assert scheduler.check_workers() == ["w2", "w3", "w4"]

    status = scheduler.job_status("job-1")
    assert (status.requeues, status.maps_done) == (0, 3)
    assert finish(scheduler, "w1", backup)  # the surviving copy completes the task
    assert scheduler.job_status("job-1").maps_done == 4


def test_task_is_requeued_once_when_both_copies_are_lost():
    scheduler, clock, slow, backup = slow_task_with_backup()

    clock.advance(6)
    scheduler.check_workers()  # everyone has gone silent

    assert scheduler.job_status("job-1").requeues == 1
    again = scheduler.get_task("w5")
    assert (again.task_id, again.attempt, again.backup) == (3, 3, False)


def test_every_lost_copy_counts_toward_the_loss_limit():
    scheduler, clock, slow, backup = slow_task_with_backup()
    clock.advance(6)
    scheduler.check_workers()  # two copies lost: w4's and w1's
    assert scheduler.job_status("job-1").state == "RUNNING"

    scheduler.get_task("w5")  # a third worker takes the task, and is lost too
    clock.advance(6)
    scheduler.check_workers()

    status = scheduler.job_status("job-1")
    assert status.state == "FAILED"
    assert "map task 3 lost its worker 3 times" in status.error


def test_workers_of_a_failed_job_are_told_to_stop():
    scheduler, _ = make(num_map=2, num_reduce=1)
    scheduler.get_task("w1")
    failing = scheduler.get_task("w2")
    assert scheduler.heartbeat("w1") is False

    scheduler.report("w2", "job-1", "map", failing.task_id, failing.attempt, False, "boom")

    assert scheduler.heartbeat("w1") is True


def test_worker_names_its_attempt_and_is_told_to_stop_a_stale_one():
    scheduler, clock, slow = four_maps_one_slow()
    running = ("job-1", "map", slow.task_id, slow.attempt)
    assert scheduler.heartbeat("w4", running) is False

    clock.advance(6)  # w4 is frozen; the boss presumes it dead and re-queues its task
    scheduler.check_workers()
    scheduler.get_task("w1")

    # w4 wakes up, still running its old attempt: it is told to stop.
    assert scheduler.heartbeat("w4", running) is True
    assert scheduler.heartbeat("w4", running) is True
    assert [w.alive for w in scheduler.workers() if w.worker_id == "w4"] == [True]


def test_failed_backup_copy_is_not_replaced():
    scheduler, clock, slow, backup = slow_task_with_backup()

    scheduler.report("w1", "job-1", "map", backup.task_id, backup.attempt, False, "disk full")
    wait(scheduler, clock, 20, alive=["w4"])  # the original is still slow

    assert scheduler.get_task("w2") is None  # one backup per hand-out


def test_requeued_task_can_get_a_backup_again():
    scheduler, clock, slow, backup = slow_task_with_backup()
    clock.advance(6)
    scheduler.check_workers()  # both copies are lost; the task is re-queued
    again = scheduler.get_task("w5")
    assert (again.task_id, again.backup) == (3, False)

    wait(scheduler, clock, 3.5, alive=["w5"])  # slow again on w5

    second = scheduler.get_task("w6")
    assert (second.task_id, second.backup) == (3, True)
