import pytest


@pytest.fixture
def shared_root(tmp_path):
    """An empty directory standing in for shared storage."""
    root = tmp_path / "shared"
    root.mkdir()
    return root
