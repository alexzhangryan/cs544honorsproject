"""The boss's scheduling logic: which task goes to which worker, and when.

This module knows nothing about gRPC or files. It is a small state machine
protected by one lock, with the clock passed in, so tests can drive it
directly and control time.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

from mapreduce.splits import Chunk

# Task states.
IDLE = "idle"
IN_PROGRESS = "in_progress"
COMPLETED = "completed"

# Job states.
RUNNING = "RUNNING"
SUCCEEDED = "SUCCEEDED"
FAILED = "FAILED"


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


@dataclass
class _Task:
    kind: str
    task_id: int
    chunks: list[Chunk] = field(default_factory=list)
    state: str = IDLE
    worker: Optional[str] = None
    attempt: int = 0  # goes up each time the task is handed out


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

    def task(self, kind: str, task_id: int) -> Optional[_Task]:
        tasks = {"map": self.maps, "reduce": self.reduces}.get(kind)
        if tasks is None or not 0 <= task_id < len(tasks):
            return None
        return tasks[task_id]


class Scheduler:
    def __init__(
        self,
        clock: Callable[[], float] = time.monotonic,
        on_event: Optional[Callable[[str, dict], None]] = None,
    ):
        self._clock = clock
        self._on_event = on_event
        self._lock = threading.Lock()
        self._jobs: dict[str, _Job] = {}  # in submission order

    def add_job(
        self,
        job_id: str,
        input_path: str,
        output_path: str,
        splits: list[list[Chunk]],
        num_reduce: int,
    ) -> None:
        """Register a job with one map task per split."""
        with self._lock:
            job = _Job(
                job_id=job_id,
                input_path=input_path,
                output_path=output_path,
                maps=[_Task("map", i, chunks) for i, chunks in enumerate(splits)],
                reduces=[_Task("reduce", i) for i in range(num_reduce)],
                submitted_at=self._clock(),
            )
            self._jobs[job_id] = job
            self._event(job, "submitted")

    def get_task(self, worker_id: str) -> Optional[Assignment]:
        """Hand the next ready task to this worker, or None if there is none.

        Jobs are served in submission order: the worker gets a task from the
        oldest job that has one ready.
        """
        with self._lock:
            for job in self._jobs.values():
                if job.state != RUNNING:
                    continue
                task = self._next_ready_task(job)
                if task is None:
                    continue
                task.state = IN_PROGRESS
                task.worker = worker_id
                task.attempt += 1
                self._event(job, "assigned", task)
                return Assignment(
                    kind=task.kind,
                    job_id=job.job_id,
                    task_id=task.task_id,
                    attempt=task.attempt,
                    num_map=len(job.maps),
                    num_reduce=len(job.reduces),
                    chunks=task.chunks,
                    output_path=job.output_path,
                )
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

        A report counts only if it is for the current attempt of a task that
        is in progress on the reporting worker. Anything else is stale.
        """
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.state != RUNNING:
                return False
            task = job.task(kind, task_id)
            if (
                task is None
                or task.state != IN_PROGRESS
                or task.attempt != attempt
                or task.worker != worker_id
            ):
                return False

            if success:
                task.state = COMPLETED
                self._event(job, "completed", task)
                if all(reduce.state == COMPLETED for reduce in job.reduces):
                    self._finish(job, SUCCEEDED)
            else:
                # An error in user code would only happen again on a retry,
                # so the whole job fails straight away.
                self._event(job, "task_failed", task)
                self._finish(job, FAILED, error or f"{kind} task {task_id} failed")
            return True

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
            )

    def events(self, job_id: str) -> list[dict]:
        """The job's scheduling log, oldest first."""
        with self._lock:
            job = self._jobs.get(job_id)
            return list(job.events) if job else []

    # Everything below is called with the lock held.

    @staticmethod
    def _next_ready_task(job: _Job) -> Optional[_Task]:
        for task in job.maps:
            if task.state == IDLE:
                return task
        if any(task.state != COMPLETED for task in job.maps):
            return None  # reduce tasks must wait for every map task
        for task in job.reduces:
            if task.state == IDLE:
                return task
        return None

    def _finish(self, job: _Job, state: str, error: str = "") -> None:
        job.state = state
        job.error = error
        job.finished_at = self._clock()
        self._event(job, "job_succeeded" if state == SUCCEEDED else "job_failed")

    def _event(self, job: _Job, name: str, task: Optional[_Task] = None) -> None:
        event = {"t": round(self._clock() - job.submitted_at, 3), "event": name}
        if task is not None:
            event.update(kind=task.kind, task=task.task_id, attempt=task.attempt, worker=task.worker)
        job.events.append(event)
        if self._on_event is not None:
            self._on_event(job.job_id, event)
