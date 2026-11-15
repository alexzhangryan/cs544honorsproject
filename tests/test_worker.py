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
def pauses(monkeypatch):
    """Every pause the worker takes, in seconds, instead of really sleeping."""
    taken = []
    monkeypatch.setattr(worker_module.time, "sleep", taken.append)
    return taken


@pytest.fixture
def worker(pauses, tmp_path):
    return Worker("127.0.0.1:1", str(tmp_path), "w1")


ASSIGNMENT = pb.TaskAssignment(kind=pb.MAP, job_id="job-1", task_id=3, attempt=1)


def test_report_that_gets_through_is_sent_once(worker, pauses):
    worker._stub = FlakyBoss(failures=0)

    worker._report(ASSIGNMENT, None)

    assert worker._stub.calls == 1
    assert pauses == []


def test_report_is_retried_until_the_boss_answers(worker, pauses):
    worker._stub = FlakyBoss(failures=1)

    worker._report(ASSIGNMENT, None)

    assert worker._stub.calls == 2  # and no third call after the one that got through
    assert pauses == [worker_module.RETRY_INTERVAL]


def test_report_gives_up_quietly_after_a_few_tries(worker, pauses):
    worker._stub = FlakyBoss(failures=100)

    worker._report(ASSIGNMENT, None)  # must not raise: the boss will re-queue the task

    assert worker._stub.calls == worker_module.REPORT_ATTEMPTS
    assert pauses == [worker_module.RETRY_INTERVAL] * (worker_module.REPORT_ATTEMPTS - 1)
