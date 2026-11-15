"""A small real cluster for tests: the boss in this process, workers as
separate processes, and a temporary directory as shared storage."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import grpc

from helpers import REPO
from mapreduce import mapreduce_pb2 as pb
from mapreduce import mapreduce_pb2_grpc as pb_grpc
from mapreduce.boss import start_boss
from mapreduce.storage import events_path


class Cluster:
    def __init__(
        self,
        shared_root: Path,
        log_dir: Path,
        port: int = 0,
        heartbeat_interval: float = 0.2,
        heartbeat_timeout: float = 3.0,
    ):
        self.shared_root = shared_root
        self.log_dir = log_dir
        self.port = port
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.boss = None
        self.stub = None
        self._channel = None
        self._workers = {}
        log_dir.mkdir(parents=True, exist_ok=True)

    def start_boss(self) -> None:
        self.boss = start_boss(
            self.shared_root,
            host="127.0.0.1",
            port=self.port,
            heartbeat_timeout=self.heartbeat_timeout,
            monitor_interval=0.1,
        )
        self.port = self.boss.port
        self._channel = grpc.insecure_channel(f"127.0.0.1:{self.port}")
        self.stub = pb_grpc.BossStub(self._channel)

    def start_worker(self, index: int) -> None:
        """Start worker number `index` as its own process (and process group)."""
        log = open(self.log_dir / f"w{index}.log", "a")
        self._workers[index] = subprocess.Popen(
            [
                sys.executable, "-m", "mapreduce.worker",
                "--boss", f"127.0.0.1:{self.port}",
                "--shared", str(self.shared_root),
                "--id", f"w{index}",
                "--heartbeat-interval", str(self.heartbeat_interval),
            ],
            cwd=REPO,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    def start_workers(self, count: int) -> None:
        for index in range(count):
            self.start_worker(index)

    def kill_worker(self, index: int) -> None:
        """Kill a worker and everything it started, as if its machine died."""
        process = self._workers.pop(index)
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
        process.wait()

    def pause_worker(self, index: int) -> None:
        """Freeze a worker and everything it started. It is not dead: it
        carries on from where it was when resumed."""
        os.killpg(self._workers[index].pid, signal.SIGSTOP)

    def resume_worker(self, index: int) -> None:
        os.killpg(self._workers[index].pid, signal.SIGCONT)

    def submit(self, job_file, input_path: str, output_path: str, maps: int, reduces: int) -> str:
        request = pb.SubmitJobRequest(
            input_path=input_path,
            output_path=output_path,
            job_source=Path(job_file).read_text(),
            num_map=maps,
            num_reduce=reduces,
        )
        return self.stub.SubmitJob(request).job_id

    def status(self, job_id: str):
        return self.stub.GetJobStatus(pb.JobStatusRequest(job_id=job_id))

    def wait(self, job_id: str, timeout: float = 60):
        """Wait until the job is no longer running and return its status."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status = self.status(job_id)
            if status.state != "RUNNING":
                return status
            time.sleep(0.05)
        raise TimeoutError(f"{job_id} still running after {timeout}s\n{self.worker_logs()}")

    def workers(self) -> dict:
        """What the boss knows about each worker, by worker id."""
        reply = self.stub.ListWorkers(pb.ListWorkersRequest())
        return {worker.worker_id: worker for worker in reply.workers}

    def wait_until(self, condition, what: str, timeout: float = 15) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if condition():
                return
            time.sleep(0.01)
        raise TimeoutError(f"timed out waiting until {what}\n{self.worker_logs()}")

    def wait_until_busy(self, *indexes: int) -> None:
        """Wait until each of the given workers is in the middle of a task."""

        def all_busy():
            workers = self.workers()
            return all(f"w{i}" in workers and workers[f"w{i}"].current_task for i in indexes)

        self.wait_until(all_busy, f"workers {indexes} are busy")

    def events(self, job_id: str) -> list:
        lines = events_path(self.shared_root, job_id).read_text().splitlines()
        return [json.loads(line) for line in lines]

    def worker_logs(self) -> str:
        return "\n".join(
            f"--- {path.name} ---\n{path.read_text()[-2000:]}" for path in sorted(self.log_dir.glob("*.log"))
        )

    def stop(self) -> None:
        for index in list(self._workers):
            self.kill_worker(index)
        if self._channel is not None:
            self._channel.close()
        if self.boss is not None:
            self.boss.stop()
