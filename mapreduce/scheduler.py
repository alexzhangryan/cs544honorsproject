"""The boss's scheduling logic: which task goes to which worker, and when.

This module knows nothing about gRPC or files. It is a small state machine
protected by one lock, with the clock passed in, so tests can drive it
directly and control time.

A task is handed out as numbered attempts. Usually one attempt runs at a
time. Two can run at once when a slow task has been given a backup copy; the
first one to report success completes the task.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from statistics import median
from typing import Callable, Optional

from mapreduce.splits import Chunk

log = logging.getLogger("boss")

# Task states.
IDLE = "idle"
IN_PROGRESS = "in_progress"
COMPLETED = "completed"

# Job states.
RUNNING = "RUNNING"
SUCCEEDED = "SUCCEEDED"
FAILED = "FAILED"

# Backup tasks (see Scheduler._find_straggler).
BACKUP_MIN_FINISHED = 3  # completed tasks of a kind needed before the rest can be judged
BACKUP_MIN_DELAY = 1.0  # seconds a task must be later than expected before it gets a backup
# A worker that has been silent for longer than this fraction of the heartbeat
# timeout may be dead, not slow. Its task is left to the timeout.
BACKUP_MAX_SILENCE = 0.4


@dataclass
class Assignment:
    """A task handed to a worker."""

    kind: str  # "map" or "reduce"
    job_id: str
    task_id: int
    attempt: int
    num_map: int
    num_reduce: int
    chunks: list[Chunk]  # map tasks only
    output_path: str
    backup: bool = False  # a second copy of a task that is running slowly elsewhere


@dataclass
class JobStatus:
    job_id: str
    state: str
    maps_done: int
    maps_total: int
    reduces_done: int
    reduces_total: int
    error: str
    elapsed: float
    requeues: int
    output_path: str
    intermediate_bytes: int = 0  # size of the map output; 0 until the map phase ends
    backups: int = 0  # backup copies started for slow tasks


@dataclass
class WorkerStatus:
    worker_id: str
    alive: bool
    current_task: str  # for example "map 3 of job-...", or "" when idle
    seconds_since_contact: float


@dataclass
class _Task:
    kind: str
    task_id: int
    chunks: list[Chunk] = field(default_factory=list)
    state: str = IDLE
    attempt: int = 0  # number of the latest attempt; goes up each time the task is handed out
    running: dict[int, str] = field(default_factory=dict)  # attempt -> worker, for copies running now
    started: dict[int, float] = field(default_factory=dict)  # attempt -> when it was handed out
    backed_up: bool = False  # a backup copy has been started since the task was last idle
    losses: int = 0  # copies of the task that were lost because their worker died
    size: int = 0  # bytes of input (for a reduce task, known once the map phase ends)


@dataclass
class _Worker:
    worker_id: str
    last_seen: float
    alive: bool = True
    # The task attempt it is running: (job id, kind, task id, attempt).
    task: Optional[tuple[str, str, int, int]] = None


@dataclass
class _Job:
    job_id: str
    input_path: str
    output_path: str
    maps: list[_Task]
    reduces: list[_Task]
    submitted_at: float
    state: str = RUNNING
    error: str = ""
    finished_at: Optional[float] = None
    requeues: int = 0
    events: list[dict] = field(default_factory=list)
    largest_first: bool = True  # hand out reduce tasks by input size, not by number
    reduce_order: list[int] = field(default_factory=list)  # indexes into reduces
    intermediate_bytes: int = 0
    backup_tasks: bool = True  # give slow tasks a backup copy
    backups: int = 0
    # (seconds taken, bytes of input) of every completed task, by kind.
    finished: dict[str, list[tuple[float, int]]] = field(
        default_factory=lambda: {"map": [], "reduce": []}
    )

    def task(self, kind: str, task_id: int) -> Optional[_Task]:
        tasks = {"map": self.maps, "reduce": self.reduces}.get(kind)
        if tasks is None or not 0 <= task_id < len(tasks):
            return None
        return tasks[task_id]


class Scheduler:
    def __init__(
        self,
        heartbeat_timeout: float = 5.0,
        max_losses: int = 3,
        backup_factor: float = 1.5,
        clock: Callable[[], float] = time.monotonic,
        on_event: Optional[Callable[[str, dict], None]] = None,
        reduce_sizes: Optional[Callable[[str, int], list[int]]] = None,
    ):
        """
        heartbeat_timeout: seconds of silence after which a worker is dead.
        max_losses: a task that loses its worker this many times fails its job.
        backup_factor: a task gets a backup copy once it has run this many
            times longer than a task of its size is expected to take.
        reduce_sizes: called with (job id, number of reduce tasks) when a
            job's map phase ends; returns the bytes of input of each reduce
            task. Without it, reduce tasks are handed out in numerical order.
        """
        self._heartbeat_timeout = heartbeat_timeout
        self._max_losses = max_losses
        self._backup_factor = backup_factor
        self._clock = clock
        self._on_event = on_event
        self._reduce_sizes = reduce_sizes
        self._lock = threading.Lock()
        self._jobs: dict[str, _Job] = {}  # in submission order
        self._workers: dict[str, _Worker] = {}

    def add_job(
        self,
        job_id: str,
        input_path: str,
        output_path: str,
        splits: list[list[Chunk]],
        num_reduce: int,
        reduce_order: str = "largest_first",
        backup_tasks: bool = True,
    ) -> None:
        """Register a job with one map task per split.

        reduce_order is "largest_first" or "numerical". backup_tasks says
        whether slow tasks of this job may be given a backup copy.
        """
        with self._lock:
            job = _Job(
                job_id=job_id,
                input_path=input_path,
                output_path=output_path,
                maps=[
                    _Task("map", i, chunks, size=sum(chunk.end - chunk.start for chunk in chunks))
                    for i, chunks in enumerate(splits)
                ],
                reduces=[_Task("reduce", i) for i in range(num_reduce)],
                submitted_at=self._clock(),
                largest_first=reduce_order != "numerical",
                reduce_order=list(range(num_reduce)),
                backup_tasks=backup_tasks,
            )
            self._jobs[job_id] = job
            self._event(job, "submitted")

    def get_task(self, worker_id: str) -> Optional[Assignment]:
        """Hand the next task to this worker, or None if there is none.

        Jobs are served in submission order: the worker gets a task from the
        oldest job that has one ready. Only if no job has a task ready is the
        worker given a backup copy of a task that is running slowly.
        """
        with self._lock:
            worker = self._contact(worker_id)
            # Workers run one task at a time and only ask when they are idle.
            # One that asks while we still show it running a task must have
            # restarted, so that task is lost.
            self._lose_task(worker)
            running_jobs = [job for job in self._jobs.values() if job.state == RUNNING]
            for job in running_jobs:
                task = self._next_ready_task(job)
                if task is not None:
                    return self._assign(job, task, worker, backup=False)
            for job in running_jobs:
                task = self._find_straggler(job, worker_id)
                if task is not None:
                    return self._assign(job, task, worker, backup=True)
            return None

    def report(
        self,
        worker_id: str,
        job_id: str,
        kind: str,
        task_id: int,
        attempt: int,
        success: bool,
        error: str = "",
    ) -> bool:
        """Record the outcome of a task. Returns False if the report is ignored.

        A report counts only if it is for an attempt that is running on the
        reporting worker. Anything else is stale: for example, a report from
        a worker that was declared dead, or from the copy of a task that
        lost the race against its backup.
        """
        with self._lock:
            worker = self._contact(worker_id)
            if worker.task == (job_id, kind, task_id, attempt):
                worker.task = None  # whatever the outcome, it is no longer running this
            job = self._jobs.get(job_id)
            if job is None or job.state != RUNNING:
                return False
            task = job.task(kind, task_id)
            if task is None or task.state != IN_PROGRESS or task.running.get(attempt) != worker_id:
                return False

            if not success:
                del task.running[attempt]
                if task.running:
                    # Another copy of the task is still running, perhaps on a
                    # healthier worker. Let it decide the task's fate.
                    self._event(job, "attempt_failed", task, attempt, worker_id)
                    return True
                # An error in user code would only happen again on a retry,
                # so the whole job fails straight away.
                self._event(job, "task_failed", task, attempt, worker_id)
                self._finish(job, FAILED, error or f"{kind} task {task_id} failed")
                return True

            job.finished[kind].append((self._clock() - task.started[attempt], task.size))
            losers = {other: holder for other, holder in task.running.items() if other != attempt}
            task.running.clear()
            task.state = COMPLETED
            self._event(job, "completed", task, attempt, worker_id)
            for other, holder in losers.items():
                # The other copy is no longer needed. Its worker finds out
                # with its next heartbeat.
                self._event(job, "cancelled", task, other, holder)
            if kind == "map" and all(other.state == COMPLETED for other in job.maps):
                self._start_reduce_phase(job)
            if all(reduce.state == COMPLETED for reduce in job.reduces):
                self._finish(job, SUCCEEDED)
            return True

    def heartbeat(self, worker_id: str, running: Optional[tuple[str, str, int, int]] = None) -> bool:
        """Note that this worker is alive.

        running is the attempt the worker says it is running: (job id, kind,
        task id, attempt). Without it, the boss goes by its own record.

        Returns True if the worker should stop that attempt, because nobody
        needs the result any more: another copy of the task has finished,
        the job has failed, or the worker was presumed dead and its task
        given to another worker. The answer stays True for as long as the
        worker keeps reporting that attempt, so a lost reply does no harm.
        """
        with self._lock:
            worker = self._contact(worker_id)
            running = running or worker.task
            if running is None:
                return False
            job_id, kind, task_id, attempt = running
            job = self._jobs.get(job_id)
            task = job.task(kind, task_id) if job is not None else None
            return not (
                job is not None
                and job.state == RUNNING
                and task is not None
                and task.running.get(attempt) == worker_id
            )

    def check_workers(self) -> list[str]:
        """Declare dead every worker that has been silent for too long, and
        put the task it was running back in the queue.

        Returns the ids of the workers that were newly declared dead. The
        boss calls this regularly from a background thread.
        """
        with self._lock:
            now = self._clock()
            newly_dead = []
            for worker in self._workers.values():
                if worker.alive and now - worker.last_seen > self._heartbeat_timeout:
                    worker.alive = False
                    newly_dead.append(worker.worker_id)
                    log.warning("worker %s has gone silent and is presumed dead", worker.worker_id)
                    self._lose_task(worker)
            return newly_dead

    def workers(self) -> list[WorkerStatus]:
        """Every worker that has ever contacted the boss, sorted by id."""
        with self._lock:
            now = self._clock()
            result = []
            for worker in sorted(self._workers.values(), key=lambda w: w.worker_id):
                current = ""
                if worker.task is not None:
                    job_id, kind, task_id, _ = worker.task
                    current = f"{kind} {task_id} of {job_id}"
                result.append(
                    WorkerStatus(worker.worker_id, worker.alive, current, now - worker.last_seen)
                )
            return result

    def job_status(self, job_id: str) -> Optional[JobStatus]:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            end = job.finished_at if job.finished_at is not None else self._clock()
            return JobStatus(
                job_id=job.job_id,
                state=job.state,
                maps_done=sum(task.state == COMPLETED for task in job.maps),
                maps_total=len(job.maps),
                reduces_done=sum(task.state == COMPLETED for task in job.reduces),
                reduces_total=len(job.reduces),
                error=job.error,
                elapsed=end - job.submitted_at,
                requeues=job.requeues,
                output_path=job.output_path,
                intermediate_bytes=job.intermediate_bytes,
                backups=job.backups,
            )

    def running_outputs(self) -> list[tuple[str, str]]:
        """(job id, output path) of every job that is still running."""
        with self._lock:
            return [(job.job_id, job.output_path) for job in self._jobs.values() if job.state == RUNNING]

    def events(self, job_id: str) -> list[dict]:
        """The job's scheduling log, oldest first."""
        with self._lock:
            job = self._jobs.get(job_id)
            return list(job.events) if job else []

    # Everything below is called with the lock held.

    def _contact(self, worker_id: str) -> _Worker:
        """Record that we heard from this worker. A dead worker that makes
        contact again is simply alive again, with nothing to do."""
        worker = self._workers.get(worker_id)
        if worker is None:
            worker = self._workers[worker_id] = _Worker(worker_id, self._clock())
        worker.last_seen = self._clock()
        worker.alive = True
        return worker

    def _assign(self, job: _Job, task: _Task, worker: _Worker, backup: bool) -> Assignment:
        """Start a new attempt of the task on this worker."""
        task.attempt += 1
        task.running[task.attempt] = worker.worker_id
        task.started[task.attempt] = self._clock()
        task.state = IN_PROGRESS
        worker.task = (job.job_id, task.kind, task.task_id, task.attempt)
        if backup:
            task.backed_up = True
            job.backups += 1
            self._event(job, "assigned", task, task.attempt, worker.worker_id, backup=True)
            log.info(
                "job %s: %s task %d is running slowly, so worker %s gets a backup copy",
                job.job_id, task.kind, task.task_id, worker.worker_id,
            )
        else:
            self._event(job, "assigned", task, task.attempt, worker.worker_id)
        return Assignment(
            kind=task.kind,
            job_id=job.job_id,
            task_id=task.task_id,
            attempt=task.attempt,
            num_map=len(job.maps),
            num_reduce=len(job.reduces),
            chunks=task.chunks,
            output_path=job.output_path,
            backup=backup,
        )

    def _lose_task(self, worker: _Worker) -> None:
        """The worker is no longer running its task (it died or restarted).
        Unless another copy of the task is still running, put the task back
        in the queue so another worker picks it up.

        Tasks the worker completed earlier are left alone: their output is
        already in shared storage.
        """
        if worker.task is None:
            return
        job_id, kind, task_id, attempt = worker.task
        worker.task = None
        job = self._jobs[job_id]
        task = job.task(kind, task_id)
        if job.state != RUNNING or task.running.get(attempt) != worker.worker_id:
            return  # nothing to recover, for example because the job already failed

        del task.running[attempt]
        task.losses += 1
        if task.running:
            # A backup copy, or the original, is still at work on this task.
            self._event(job, "lost", task, attempt, worker.worker_id)
        else:
            self._event(job, "requeued", task, attempt, worker.worker_id)
            log.warning(
                "job %s: %s task %d was lost with worker %s and is back in the queue",
                job_id, kind, task_id, worker.worker_id,
            )
            task.state = IDLE
            task.backed_up = False
            job.requeues += 1
        if task.losses >= self._max_losses:
            # A task that takes down every worker it runs on would otherwise
            # be retried forever. Every lost copy counts, including one whose
            # twin was still running, or such a task could use up two
            # workers at a time without ever reaching the limit.
            self._finish(
                job, FAILED, f"{kind} task {task_id} lost its worker {task.losses} times; giving up"
            )

    @staticmethod
    def _next_ready_task(job: _Job) -> Optional[_Task]:
        for task in job.maps:
            if task.state == IDLE:
                return task
        if any(task.state != COMPLETED for task in job.maps):
            return None  # reduce tasks must wait for every map task
        for index in job.reduce_order:
            if job.reduces[index].state == IDLE:
                return job.reduces[index]
        return None

    def _find_straggler(self, job: _Job, worker_id: str) -> Optional[_Task]:
        """Pick a task of this job that deserves a backup copy, if any.

        A slow worker holds up a whole job, because the job waits for that
        worker's last task. A free worker can run a second copy of the task
        instead of sitting idle.

        Slow means slow for its size. Tasks differ in how much input they
        have, so the comparison is by pace: seconds per byte of input. The
        completed tasks of the same kind give the normal pace (their
        median; tasks without input have no pace and are left out). A
        running task qualifies when it has already run backup_factor times
        longer than its size explains at that pace, and is at least a second
        late.

        Backups are for workers that are alive but slow. A task whose worker
        has gone silent is skipped: that worker is probably dead, and the
        heartbeat timeout will put the task back in the queue.
        """
        if not job.backup_tasks:
            return None
        in_map_phase = any(task.state != COMPLETED for task in job.maps)
        tasks, kind = (job.maps, "map") if in_map_phase else (job.reduces, "reduce")
        finished = [(seconds, size) for seconds, size in job.finished[kind] if size > 0]
        if len(finished) < BACKUP_MIN_FINISHED:
            return None  # too early to say what normal is
        pace = median(seconds / size for seconds, size in finished)

        now = self._clock()
        worst, worst_delay = None, 0.0
        for task in tasks:
            if task.state != IN_PROGRESS or task.backed_up or len(task.running) != 1:
                continue
            ((attempt, holder),) = task.running.items()
            if holder == worker_id:
                continue
            silence = now - self._workers[holder].last_seen
            if silence > self._heartbeat_timeout * BACKUP_MAX_SILENCE:
                continue
            age = now - task.started[attempt]
            expected = pace * task.size
            if age < expected + BACKUP_MIN_DELAY or age <= self._backup_factor * expected:
                continue
            if worst is None or age - expected > worst_delay:
                worst, worst_delay = task, age - expected
        return worst

    def _start_reduce_phase(self, job: _Job) -> None:
        """The last map task has just completed. Find out how much input each
        reduce task has, and decide the order to hand them out in.

        The largest goes first. A job has to wait for its longest reduce
        task whatever happens, so that one should start as early as possible.
        """
        sizes = None
        if self._reduce_sizes is not None:
            try:
                sizes = list(self._reduce_sizes(job.job_id, len(job.reduces)))
            except Exception as error:
                log.error("could not measure the reduce inputs of job %s: %s", job.job_id, error)
        if sizes is None or len(sizes) != len(job.reduces):
            self._event(job, "map_phase_done")
            return  # carry on in numerical order

        for task, size in zip(job.reduces, sizes):
            task.size = size
        job.intermediate_bytes = sum(sizes)
        if job.largest_first:
            # Sorting is stable, so tasks of equal size keep their numerical order.
            job.reduce_order = sorted(range(len(sizes)), key=lambda index: -sizes[index])
        self._event(job, "map_phase_done", reduce_sizes=sizes)

    def _finish(self, job: _Job, state: str, error: str = "") -> None:
        job.state = state
        job.error = error
        job.finished_at = self._clock()
        self._event(job, "job_succeeded" if state == SUCCEEDED else "job_failed")
        elapsed = job.finished_at - job.submitted_at
        if state == SUCCEEDED:
            log.info("job %s SUCCEEDED after %.1fs", job.job_id, elapsed)
        else:
            reason = (error.strip().splitlines() or ["unknown error"])[-1]
            log.warning("job %s FAILED after %.1fs: %s", job.job_id, elapsed, reason)

    def _event(
        self,
        job: _Job,
        name: str,
        task: Optional[_Task] = None,
        attempt: Optional[int] = None,
        worker_id: Optional[str] = None,
        **details,
    ) -> None:
        event = {"t": round(self._clock() - job.submitted_at, 3), "event": name}
        if task is not None:
            event.update(kind=task.kind, task=task.task_id, attempt=attempt, worker=worker_id)
        event.update(details)
        job.events.append(event)
        if self._on_event is not None:
            try:
                self._on_event(job.job_id, event)
            except Exception as error:
                # The hook only keeps a record for people (the boss writes a
                # log file). Scheduling must carry on if that record cannot
                # be written, for example because the job's directory is gone.
                log.error("could not record a %r event for job %s: %s", name, job.job_id, error)
