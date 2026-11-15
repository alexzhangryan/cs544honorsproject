"""End-to-end tests on the real Docker Compose cluster.

These are skipped by default. Run them with:  python -m pytest -m docker
"""

import shutil
import subprocess
import time
import uuid

import grpc
import pytest

from helpers import REPO, WORDCOUNT, read_output, reference_wordcount
from mapreduce import mapreduce_pb2 as pb
from mapreduce import mapreduce_pb2_grpc as pb_grpc
from mapreduce.client import main, submit_job, wait_for_job

pytestmark = pytest.mark.docker

SHARED = REPO / "shared"
JOBS = SHARED / "jobs"
SAMPLE = REPO / "data" / "sample"
WORKERS = [f"mr-worker-{n}" for n in range(1, 5)]


def docker(*args, check=True):
    return subprocess.run(["docker", *args], cwd=REPO, check=check, capture_output=True, text=True)


def client(*args):
    return main(["--shared", str(SHARED), *args])


def list_workers():
    with grpc.insecure_channel("localhost:50051") as channel:
        reply = pb_grpc.BossStub(channel).ListWorkers(pb.ListWorkersRequest(), timeout=5)
    return {worker.worker_id: worker for worker in reply.workers}


def wait_until(condition, what, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.05)
    raise TimeoutError(f"timed out waiting until {what}")


def wait_for_boss(timeout=60):
    deadline = time.monotonic() + timeout
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = pb_grpc.BossStub(channel)
        while time.monotonic() < deadline:
            try:
                stub.GetJobStatus(pb.JobStatusRequest(job_id="none"), timeout=2)
            except grpc.RpcError as error:
                if error.code() == grpc.StatusCode.NOT_FOUND:
                    return  # the boss answered
            time.sleep(0.5)
    raise TimeoutError("the boss did not come up")


@pytest.fixture(scope="module")
def compose():
    """The Compose cluster. Started (and afterwards stopped) only if it was not already up."""
    already_up = bool(docker("compose", "ps", "-q").stdout.strip())
    docker("compose", "up", "-d", "--build")
    wait_for_boss()
    yield
    if not already_up:
        docker("compose", "down")


@pytest.fixture
def names(compose):
    """Unique input and output locations in shared storage, removed afterwards."""
    tag = uuid.uuid4().hex[:8]
    paths = {"input": f"input/e2e-{tag}", "output": f"output/e2e-{tag}"}
    jobs_before = set(JOBS.iterdir()) if JOBS.exists() else set()
    yield paths
    for rel in paths.values():
        shutil.rmtree(SHARED / rel, ignore_errors=True)
    for job in set(JOBS.iterdir()) - jobs_before:
        shutil.rmtree(job, ignore_errors=True)


def test_wordcount_on_compose_cluster(names):
    texts = {path.name: path.read_text() for path in SAMPLE.glob("*.txt")}
    assert texts, "the sample dataset is missing"

    assert client("upload", str(SAMPLE), names["input"]) == 0
    assert client(
        "submit", "--input", names["input"], "--output", names["output"],
        "--job", str(WORDCOUNT), "--maps", "8", "--reduces", "4",
    ) == 0

    assert read_output(SHARED, names["output"]) == reference_wordcount(texts)
    assert len(list((SHARED / names["output"]).glob("part-*.txt"))) == 4


def test_each_worker_is_limited_to_one_cpu(compose):
    for worker in WORKERS:
        nano_cpus = docker("inspect", "--format", "{{.HostConfig.NanoCpus}}", worker).stdout.strip()
        assert nano_cpus == "1000000000", f"{worker} is not limited to one CPU"


# Word count that sleeps a little on every line, so that a map task takes
# a few seconds and a worker can be killed in the middle of one.
SLOW_WORDCOUNT = (
    "import time\n"
    "def map_fn(key, value):\n"
    "    time.sleep(0.003)\n"
    "    for word in value.split():\n"
    "        yield word.lower(), 1\n"
    "def reduce_fn(key, values):\n"
    "    yield key, sum(values)\n"
)


def test_compose_survives_docker_kill(names, tmp_path):
    texts = {path.name: path.read_text() for path in SAMPLE.glob("*.txt")}
    job_file = tmp_path / "slow_wordcount.py"
    job_file.write_text(SLOW_WORDCOUNT)
    assert client("upload", str(SAMPLE), names["input"]) == 0

    with grpc.insecure_channel("localhost:50051") as channel:
        stub = pb_grpc.BossStub(channel)
        job_id = submit_job(stub, names["input"], names["output"], job_file, maps=8, reduces=4)
        wait_until(lambda: list_workers()["worker-2"].current_task, "worker-2 is in the middle of a task")
        docker("kill", "mr-worker-2")
        try:
            status = wait_for_job(stub, job_id)
        finally:
            docker("start", "mr-worker-2")

    assert status.state == "SUCCEEDED", status.error
    assert status.requeues >= 1
    assert read_output(SHARED, names["output"]) == reference_wordcount(texts)
    assert sorted(path.name for path in (SHARED / names["output"]).iterdir()) == [
        f"part-0000{r}.txt" for r in range(4)
    ]
    wait_until(lambda: list_workers()["worker-2"].alive, "worker-2 is alive again after being restarted")
