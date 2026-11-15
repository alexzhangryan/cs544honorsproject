"""The boss: a gRPC server in front of the scheduler.

The boss accepts jobs from the client, splits their input, and hands tasks
to workers that ask for them. It never calls a worker. A background thread
watches for workers that have gone silent, so their tasks can be given to
someone else.

Run it with:  python -m mapreduce.boss
"""

from __future__ import annotations

import argparse
import ast
import json
import logging
import os
import secrets
import shutil
import threading
import time
from concurrent import futures
from dataclasses import dataclass
from pathlib import Path

import grpc

from mapreduce import mapreduce_pb2 as pb
from mapreduce import mapreduce_pb2_grpc as pb_grpc
from mapreduce.scheduler import SUCCEEDED, Scheduler
from mapreduce.splits import list_input_files, make_splits
from mapreduce.storage import events_path, intermediate_dir, job_source_path, resolve_inside

log = logging.getLogger("boss")

KIND_TO_PROTO = {"map": pb.MAP, "reduce": pb.REDUCE}
PROTO_TO_KIND = {pb.MAP: "map", pb.REDUCE: "reduce"}


class BossServicer(pb_grpc.BossServicer):
    def __init__(self, scheduler: Scheduler, shared_root: Path):
        self._scheduler = scheduler
        self._shared_root = shared_root

    # Called by the client.

    def SubmitJob(self, request, context):
        try:
            job_id = self._submit(request)
        except ValueError as error:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))
        log.info(
            "job %s submitted: input=%s output=%s maps=%d reduces=%d",
            job_id, request.input_path, request.output_path, request.num_map, request.num_reduce,
        )
        return pb.SubmitJobReply(job_id=job_id)

    def _submit(self, request) -> str:
        """Check the request, set up the job's directory and queue the job.

        Raises ValueError with a one-line message if the request is bad.
        Nothing is created until every check has passed.
        """
        if request.num_map < 1:
            raise ValueError("the number of map tasks must be at least 1")
        if request.num_reduce < 1:
            raise ValueError("the number of reduce tasks must be at least 1")

        resolve_inside(self._shared_root, request.input_path)
        try:
            files = list_input_files(self._shared_root, request.input_path)
        except FileNotFoundError:
            raise ValueError(f"input path not found in shared storage: {request.input_path!r}")
        if not files:
            raise ValueError(f"no input files found under {request.input_path!r}")

        output = resolve_inside(self._shared_root, request.output_path)
        if output.exists() and (not output.is_dir() or any(output.iterdir())):
            raise ValueError(
                f"output directory already exists and is not empty: {request.output_path!r}"
            )

        try:
            ast.parse(request.job_source)
        except SyntaxError as error:
            raise ValueError(f"the job file has a syntax error on line {error.lineno}: {error.msg}")

        job_id = time.strftime("job-%Y%m%d-%H%M%S-") + secrets.token_hex(2)
        intermediate_dir(self._shared_root, job_id).mkdir(parents=True)
        job_source_path(self._shared_root, job_id).write_text(request.job_source, encoding="utf-8")
        self._scheduler.add_job(
            job_id,
            request.input_path,
            request.output_path,
            make_splits(files, request.num_map),
            request.num_reduce,
        )
        return job_id

    def GetJobStatus(self, request, context):
        status = self._scheduler.job_status(request.job_id)
        if status is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"unknown job: {request.job_id!r}")
        return pb.JobStatusReply(
            job_id=status.job_id,
            state=status.state,
            maps_done=status.maps_done,
            maps_total=status.maps_total,
            reduces_done=status.reduces_done,
            reduces_total=status.reduces_total,
            error=status.error,
            elapsed_seconds=status.elapsed,
            requeues=status.requeues,
            output_path=status.output_path,
        )

    def ListWorkers(self, request, context):
        return pb.ListWorkersReply(
            workers=[
                pb.WorkerInfo(
                    worker_id=worker.worker_id,
                    alive=worker.alive,
                    current_task=worker.current_task,
                    seconds_since_contact=worker.seconds_since_contact,
                )
                for worker in self._scheduler.workers()
            ]
        )

    # Called by workers.

    def GetTask(self, request, context):
        assignment = self._scheduler.get_task(request.worker_id)
        if assignment is None:
            return pb.TaskAssignment(kind=pb.NONE)
        return pb.TaskAssignment(
            kind=KIND_TO_PROTO[assignment.kind],
            job_id=assignment.job_id,
            task_id=assignment.task_id,
            attempt=assignment.attempt,
            num_map=assignment.num_map,
            num_reduce=assignment.num_reduce,
            chunks=[pb.Chunk(path=c.path, start=c.start, end=c.end) for c in assignment.chunks],
            output_path=assignment.output_path,
        )

    def Heartbeat(self, request, context):
        self._scheduler.heartbeat(request.worker_id)
        return pb.HeartbeatReply()

    def ReportTask(self, request, context):
        accepted = self._scheduler.report(
            request.worker_id,
            request.job_id,
            PROTO_TO_KIND.get(request.kind, ""),
            request.task_id,
            request.attempt,
            request.success,
            request.error,
        )
        if accepted:
            status = self._scheduler.job_status(request.job_id)
            if status.state == SUCCEEDED:
                # The intermediate files have done their job.
                shutil.rmtree(intermediate_dir(self._shared_root, request.job_id), ignore_errors=True)
            if status.state != "RUNNING":
                log.info("job %s %s after %.1fs", status.job_id, status.state, status.elapsed)
        return pb.ReportTaskReply(accepted=accepted)


def event_writer(shared_root: Path):
    """Return a function that appends a job's scheduling events to its events.jsonl."""

    def write(job_id: str, event: dict) -> None:
        with open(events_path(shared_root, job_id), "a", encoding="utf-8") as f:
            f.write(json.dumps(event) + "\n")

    return write


def watch_workers(scheduler: Scheduler, interval: float, stop: threading.Event) -> None:
    """Regularly check for workers that have stopped sending heartbeats."""
    while not stop.wait(interval):
        scheduler.check_workers()


@dataclass
class BossHandle:
    """A running boss, as returned by start_boss."""

    server: grpc.Server
    scheduler: Scheduler
    port: int
    _stop_watching: threading.Event

    def stop(self) -> None:
        self._stop_watching.set()
        self.server.stop(grace=None).wait()


def start_boss(
    shared_root: Path,
    host: str = "0.0.0.0",
    port: int = 50051,
    heartbeat_timeout: float = 5.0,
    monitor_interval: float = 0.5,
) -> BossHandle:
    """Start the boss in background threads and return a handle to it.

    Pass port=0 to let the system pick a free port (see BossHandle.port).
    heartbeat_timeout is how long a worker may stay silent before it is
    presumed dead; monitor_interval is how often that is checked.
    """
    shared_root = Path(shared_root)
    scheduler = Scheduler(heartbeat_timeout=heartbeat_timeout, on_event=event_writer(shared_root))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    pb_grpc.add_BossServicer_to_server(BossServicer(scheduler, shared_root), server)
    bound_port = server.add_insecure_port(f"{host}:{port}")
    server.start()

    stop_watching = threading.Event()
    threading.Thread(
        target=watch_workers, args=(scheduler, monitor_interval, stop_watching), daemon=True
    ).start()
    return BossHandle(server=server, scheduler=scheduler, port=bound_port, _stop_watching=stop_watching)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the MapReduce boss.")
    parser.add_argument("--shared", default=os.environ.get("MR_SHARED", "/shared"),
                        help="shared storage directory (default: $MR_SHARED or /shared)")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--heartbeat-timeout", type=float,
                        default=float(os.environ.get("MR_HEARTBEAT_TIMEOUT", "5")),
                        help="seconds of silence after which a worker is presumed dead "
                             "(default: $MR_HEARTBEAT_TIMEOUT or 5)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s: %(message)s")
    handle = start_boss(Path(args.shared), port=args.port, heartbeat_timeout=args.heartbeat_timeout)
    log.info(
        "listening on port %d, shared storage at %s, heartbeat timeout %.1fs",
        handle.port, args.shared, args.heartbeat_timeout,
    )
    handle.server.wait_for_termination()


if __name__ == "__main__":
    main()
