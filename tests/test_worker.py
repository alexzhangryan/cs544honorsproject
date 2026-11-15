"""The worker's dealings with the boss, with the boss replaced by a fake."""

import grpc
import pytest

from mapreduce import mapreduce_pb2 as pb
from mapreduce import worker as worker_module
from mapreduce.worker import Worker


class Unavailable(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.UNAVAILABLE


class FlakyBoss:
    """Fails the first `failures` ReportTask calls, then accepts."""

    def __init__(self, failures):
        self.failures = failures
        self.calls = 0

    def ReportTask(self, request, timeout=None):
        self.calls += 1
        if self.calls <= self.failures:
            raise Unavailable()
        return pb.ReportTaskReply(accepted=True)


@pytest.fixture
def worker(monkeypatch, tmp_path):
    monkeypatch.setattr(worker_module.time, "sleep", lambda seconds: None)
    return Worker("127.0.0.1:1", str(tmp_path), "w1")


ASSIGNMENT = pb.TaskAssignment(kind=pb.MAP, job_id="job-1", task_id=3, attempt=1)


def test_report_is_retried_until_the_boss_answers(worker):
    worker._stub = FlakyBoss(failures=2)

    worker._report(ASSIGNMENT, None)

    assert worker._stub.calls == 3


def test_report_gives_up_quietly_after_a_few_tries(worker):
    worker._stub = FlakyBoss(failures=100)

    worker._report(ASSIGNMENT, None)  # must not raise: the boss will re-queue the task

    assert worker._stub.calls == worker_module.REPORT_ATTEMPTS
