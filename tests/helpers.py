"""Helpers shared by the tests."""

from __future__ import annotations

import shutil
from collections import Counter
from pathlib import Path

from mapreduce.splits import list_input_files, make_splits
from mapreduce.storage import job_source_path
from mapreduce.tasks import run_task

REPO = Path(__file__).resolve().parent.parent
WORDCOUNT = REPO / "examples" / "wordcount.py"

TEXTS = {
    "input/books/one.txt": "the quick brown fox\njumps over the lazy dog\n",
    "input/books/two.txt": "The dog barks\nthe fox runs away\n\nthe end",
}


def write_input(shared_root: Path, texts: dict) -> None:
    for rel, text in texts.items():
        path = shared_root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")


def reference_wordcount(texts: dict) -> dict:
    """Word counts computed without MapReduce, as output-style strings."""
    counts = Counter()
    for text in texts.values():
        for line in text.splitlines():
            for word in line.split():
                counts[word.lower()] += 1
    return {word: str(count) for word, count in counts.items()}


def read_output(shared_root: Path, output_path: str) -> dict:
    """Parse every part file. Fails if a key appears more than once."""
    result = {}
    for part in sorted((shared_root / output_path).glob("part-*.txt")):
        for line in part.read_text(encoding="utf-8").splitlines():
            key, value = line.split("\t", 1)
            assert key not in result, f"key {key!r} appears more than once"
            result[key] = value
    return result


def install_job(shared_root: Path, job_id: str, job_file: Path) -> None:
    """Set up a job's directory in shared storage, as the boss does on submit."""
    from mapreduce.storage import intermediate_dir

    target = job_source_path(shared_root, job_id)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(job_file, target)
    intermediate_dir(shared_root, job_id).mkdir(exist_ok=True)


def map_task(job_id, map_id, chunks, num_map, num_reduce, attempt=1, worker_id="w1") -> dict:
    return {
        "kind": "map",
        "job_id": job_id,
        "task_id": map_id,
        "attempt": attempt,
        "num_map": num_map,
        "num_reduce": num_reduce,
        "chunks": [[c.path, c.start, c.end] for c in chunks],
        "output_path": "",
        "worker_id": worker_id,
    }


def reduce_task(job_id, reduce_id, num_map, num_reduce, output_path, attempt=1, worker_id="w1") -> dict:
    return {
        "kind": "reduce",
        "job_id": job_id,
        "task_id": reduce_id,
        "attempt": attempt,
        "num_map": num_map,
        "num_reduce": num_reduce,
        "chunks": [],
        "output_path": output_path,
        "worker_id": worker_id,
    }


def run_job_locally(shared_root, job_file, input_path, output_path, num_map, num_reduce, job_id="job-test"):
    """Run a whole job in this process, one task after another."""
    install_job(shared_root, job_id, job_file)
    splits = make_splits(list_input_files(shared_root, input_path), num_map)
    for map_id, chunks in enumerate(splits):
        run_task(str(shared_root), map_task(job_id, map_id, chunks, num_map, num_reduce))
    for reduce_id in range(num_reduce):
        run_task(str(shared_root), reduce_task(job_id, reduce_id, num_map, num_reduce, output_path))


def as_task(assignment, worker_id: str) -> dict:
    """Turn a TaskAssignment message into the dict that run_task takes."""
    from mapreduce import mapreduce_pb2
    from mapreduce.splits import Chunk

    if assignment.kind == mapreduce_pb2.MAP:
        chunks = [Chunk(c.path, c.start, c.end) for c in assignment.chunks]
        return map_task(
            assignment.job_id, assignment.task_id, chunks, assignment.num_map,
            assignment.num_reduce, assignment.attempt, worker_id,
        )
    return reduce_task(
        assignment.job_id, assignment.task_id, assignment.num_map, assignment.num_reduce,
        assignment.output_path, assignment.attempt, worker_id,
    )


WORDCOUNT_COMBINER = REPO / "examples" / "wordcount_combiner.py"


def run_map_phase(shared_root, job_file, input_path, num_map, num_reduce, job_id="job-test"):
    """Run only the map tasks of a job, and return every intermediate pair written."""
    import json

    from mapreduce.storage import intermediate_dir

    install_job(shared_root, job_id, job_file)
    splits = make_splits(list_input_files(shared_root, input_path), num_map)
    for map_id, chunks in enumerate(splits):
        run_task(str(shared_root), map_task(job_id, map_id, chunks, num_map, num_reduce))
    pairs = []
    for path in sorted(intermediate_dir(shared_root, job_id).glob("map-*.jsonl")):
        pairs.extend(tuple(json.loads(line)) for line in path.read_text().splitlines())
    return pairs
