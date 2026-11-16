"""A worker: asks the boss for tasks and runs them, one at a time.

Each task runs in a child process. The worker itself only talks to the boss:
it waits for the child and sends a heartbeat every second while it waits.
This keeps heartbeats on time no matter how busy the user's code is, and a
crash in user code is reported as a failed task instead of killing the
worker.

Run it with:  python -m mapreduce.worker
"""

from __future__ import annotations

import argparse
import logging
import multiprocessing
import os
import socket
import time
from typing import Optional

import grpc

from mapreduce import mapreduce_pb2 as pb
from mapreduce import mapreduce_pb2_grpc as pb_grpc
from mapreduce.tasks import child_main

log = logging.getLogger("worker")

KIND_NAMES = {pb.MAP: "map", pb.REDUCE: "reduce"}
RPC_TIMEOUT = 5.0  # seconds to wait for the boss to answer one call
RETRY_INTERVAL = 1.0  # seconds between attempts while the boss is unreachable
REPORT_ATTEMPTS = 3  # how many times to try to deliver a task's result

# Without these, gRPC waits longer and longer (up to two minutes) between
# attempts to reconnect, so a worker started before the boss would be slow
# to notice that the boss has come up.
CHANNEL_OPTIONS = [
    ("grpc.initial_reconnect_backoff_ms", 500),
    ("grpc.min_reconnect_backoff_ms", 500),
    ("grpc.max_reconnect_backoff_ms", 2000),
]


class Worker:
    def __init__(
        self,
        boss_addr: str,
        shared_root: str,
        worker_id: str,
        heartbeat_interval: float = 1.0,
        poll_interval: float = 0.2,
    ):
        self.shared_root = shared_root
        self.worker_id = worker_id
        self.heartbeat_interval = heartbeat_interval
        self.poll_interval = poll_interval
        self._stub = pb_grpc.BossStub(grpc.insecure_channel(boss_addr, options=CHANNEL_OPTIONS))
        # Task processes must not be forked from this process, because
        # forking a process that has gRPC threads running is not safe. A fork
        # server is a clean helper process that does the forking instead.
        methods = multiprocessing.get_all_start_methods()
        self._mp = multiprocessing.get_context("forkserver" if "forkserver" in methods else "spawn")

    def run_forever(self) -> None:
        """Ask for a task, run it, report the result, and repeat."""
        connected = True
        while True:
            try:
                assignment = self._stub.GetTask(
                    pb.GetTaskRequest(worker_id=self.worker_id), timeout=RPC_TIMEOUT
                )
            except grpc.RpcError as error:
                if connected:
                    log.warning("cannot reach the boss (%s); will keep trying", error.code().name)
                    connected = False
                time.sleep(RETRY_INTERVAL)
                continue
            if not connected:
                log.info("connected to the boss")
                connected = True

            if assignment.kind == pb.NONE:
                time.sleep(self.poll_interval)
                continue

            name = f"{KIND_NAMES[assignment.kind]} {assignment.task_id} of {assignment.job_id}"
            log.info("starting %s (attempt %d)", name, assignment.attempt)
            started = time.monotonic()
            error = self._run(assignment)
            if error is None:
                log.info("finished %s in %.1fs", name, time.monotonic() - started)
            else:
                log.warning("%s failed:\n%s", name, error)
            self._report(assignment, error)

    def _run(self, assignment) -> Optional[str]:
        """Run one task in a child process, sending heartbeats while it runs.

        Returns None if the task succeeded, otherwise the error text.
        """
        task = {
            "kind": KIND_NAMES[assignment.kind],
            "job_id": assignment.job_id,
            "task_id": assignment.task_id,
            "attempt": assignment.attempt,
            "num_map": assignment.num_map,
            "num_reduce": assignment.num_reduce,
            "chunks": [[c.path, c.start, c.end] for c in assignment.chunks],
            "output_path": assignment.output_path,
            "worker_id": self.worker_id,
        }
        receiver, sender = self._mp.Pipe(duplex=False)
        process = self._mp.Process(target=child_main, args=(self.shared_root, task, sender))
        process.start()
        sender.close()  # only the child holds the sending end now
        try:
            while not receiver.poll(self.heartbeat_interval):
                self._heartbeat()
            try:
                return receiver.recv()
            except EOFError:
                # The pipe closed with nothing sent: the child is gone.
                process.join()
                return f"the task process died without reporting (exit code {process.exitcode})"
        finally:
            process.join()
            receiver.close()

    def _heartbeat(self) -> None:
        try:
            self._stub.Heartbeat(pb.HeartbeatRequest(worker_id=self.worker_id), timeout=RPC_TIMEOUT)
        except grpc.RpcError:
            pass  # if this keeps failing, the boss will give our task to someone else

    def _report(self, assignment, error: Optional[str]) -> None:
        request = pb.ReportTaskRequest(
            worker_id=self.worker_id,
            job_id=assignment.job_id,
            kind=assignment.kind,
            task_id=assignment.task_id,
            attempt=assignment.attempt,
            success=error is None,
            error=error or "",
        )
        # A finished task whose report is lost has to be run again, so one
        # failed call is not enough reason to give up.
        for attempt in range(1, REPORT_ATTEMPTS + 1):
            try:
                self._stub.ReportTask(request, timeout=RPC_TIMEOUT)
                return
            except grpc.RpcError as rpc_error:
                log.warning(
                    "could not report to the boss (%s), try %d of %d",
                    rpc_error.code().name, attempt, REPORT_ATTEMPTS,
                )
                if attempt < REPORT_ATTEMPTS:
                    time.sleep(RETRY_INTERVAL)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a MapReduce worker.")
    parser.add_argument("--boss", default=os.environ.get("MR_BOSS", "boss:50051"),
                        help="address of the boss (default: $MR_BOSS or boss:50051)")
    parser.add_argument("--shared", default=os.environ.get("MR_SHARED", "/shared"),
                        help="shared storage directory (default: $MR_SHARED or /shared)")
    parser.add_argument("--id", default=os.environ.get("MR_WORKER_ID") or socket.gethostname(),
                        help="name of this worker (default: $MR_WORKER_ID or the host name)")
    parser.add_argument("--heartbeat-interval", type=float,
                        default=float(os.environ.get("MR_HEARTBEAT_INTERVAL", "1.0")),
                        help="seconds between heartbeats while a task runs (default: 1.0)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format=f"%(asctime)s {args.id}: %(message)s")
    log.info("started; boss at %s, shared storage at %s", args.boss, args.shared)
    worker = Worker(args.boss, args.shared, args.id, args.heartbeat_interval)
    try:
        worker.run_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
