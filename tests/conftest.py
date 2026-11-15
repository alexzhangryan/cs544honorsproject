import os

# The tests start worker processes from a process that also runs a gRPC
# server. Without this, gRPC logs a notice about fork handlers every time.
os.environ.setdefault("GRPC_ENABLE_FORK_SUPPORT", "0")

import pytest  # noqa: E402

from cluster import Cluster  # noqa: E402


@pytest.fixture
def shared_root(tmp_path):
    """An empty directory standing in for shared storage."""
    root = tmp_path / "shared"
    root.mkdir()
    return root


@pytest.fixture
def cluster(shared_root, tmp_path):
    """A running boss with no workers yet; call cluster.start_workers(n)."""
    cluster = Cluster(shared_root, tmp_path / "logs")
    cluster.start_boss()
    yield cluster
    cluster.stop()
