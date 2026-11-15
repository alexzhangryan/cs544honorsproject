"""Where things live in shared storage.

    input/<dataset>/...                          uploaded by the client
    output/<name>/part-00000.txt ...             one file per reduce task
    jobs/<job-id>/job.py                         the user's job file
    jobs/<job-id>/events.jsonl                   the boss's scheduling log
    jobs/<job-id>/intermediate/                  removed when the job succeeds
        map-<m>-reduce-<r>.jsonl

Input and output locations are chosen by the user and are given as paths
relative to the shared storage root.
"""

from __future__ import annotations

from pathlib import Path


def resolve_inside(shared_root: Path, rel: str) -> Path:
    """Turn a path relative to shared storage into a real path.

    Raises ValueError if the path is absolute, empty, or leads outside
    shared storage.
    """
    if not rel or Path(rel).is_absolute():
        raise ValueError(f"path must be relative to shared storage: {rel!r}")
    root = shared_root.resolve()
    full = (root / rel).resolve()
    if root not in full.parents:
        raise ValueError(f"path must stay inside shared storage: {rel!r}")
    return full


def job_dir(shared_root: Path, job_id: str) -> Path:
    return shared_root / "jobs" / job_id


def job_source_path(shared_root: Path, job_id: str) -> Path:
    return job_dir(shared_root, job_id) / "job.py"


def events_path(shared_root: Path, job_id: str) -> Path:
    return job_dir(shared_root, job_id) / "events.jsonl"


def intermediate_dir(shared_root: Path, job_id: str) -> Path:
    return job_dir(shared_root, job_id) / "intermediate"


def intermediate_path(shared_root: Path, job_id: str, map_id: int, reduce_id: int) -> Path:
    return intermediate_dir(shared_root, job_id) / f"map-{map_id}-reduce-{reduce_id}.jsonl"


def output_part_path(shared_root: Path, output_path: str, reduce_id: int) -> Path:
    return shared_root / output_path / f"part-{reduce_id:05d}.txt"
