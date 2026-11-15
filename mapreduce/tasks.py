"""The work a worker does for one map task or one reduce task.

Nothing here talks to the boss. A task reads and writes files in shared
storage and either returns or raises.

Every file is written under a temporary name and renamed into place only
when it is complete. A task that is killed half way therefore leaves no file
that another task could mistake for finished output, and running the same
task twice simply replaces a file with an identical one.
"""

from __future__ import annotations

import json
import os
import traceback
import types
import zlib
from itertools import groupby
from operator import itemgetter
from pathlib import Path

from mapreduce.splits import Chunk, read_lines
from mapreduce.storage import (
    intermediate_dir,
    intermediate_path,
    job_source_path,
    output_part_path,
)


class JobError(Exception):
    """The user's job file cannot be used."""


def partition(key: str, num_reduce: int) -> int:
    """The reduce task that handles this key.

    CRC32 is used instead of the built-in hash() because hash() of a string
    differs from one Python process to the next.
    """
    return zlib.crc32(key.encode("utf-8")) % num_reduce


def load_job(path: Path) -> types.ModuleType:
    """Run the user's job file and return it as a module with map_fn and reduce_fn."""
    module = types.ModuleType("mapreduce_job")
    module.__file__ = str(path)
    source = path.read_text(encoding="utf-8")
    exec(compile(source, str(path), "exec"), module.__dict__)
    for name in ("map_fn", "reduce_fn"):
        if not callable(getattr(module, name, None)):
            raise JobError(f"the job file does not define a function named {name}")
    return module


def run_map(
    shared_root: Path,
    job_id: str,
    map_id: int,
    chunks: list[Chunk],
    num_reduce: int,
    tmp_suffix: str,
) -> None:
    """Call map_fn on every line of the chunks and write one intermediate
    file per reduce task."""
    job = load_job(job_source_path(shared_root, job_id))
    finals = [intermediate_path(shared_root, job_id, map_id, r) for r in range(num_reduce)]
    temps = [Path(str(final) + tmp_suffix) for final in finals]
    intermediate_dir(shared_root, job_id).mkdir(parents=True, exist_ok=True)

    files = []
    try:
        for temp in temps:
            files.append(open(temp, "w", encoding="utf-8"))
        for chunk in chunks:
            for line in read_lines(shared_root, chunk):
                for key, value in job.map_fn(chunk.path, line) or ():
                    if not isinstance(key, str):
                        raise TypeError(
                            "intermediate keys must be strings, but map_fn yielded "
                            f"a key of type {type(key).__name__}"
                        )
                    files[partition(key, num_reduce)].write(json.dumps([key, value]) + "\n")
        for f in files:
            f.close()
        for temp, final in zip(temps, finals):
            os.replace(temp, final)
    except BaseException:
        for f in files:
            f.close()
        for temp in temps:
            temp.unlink(missing_ok=True)
        raise


def run_reduce(
    shared_root: Path,
    job_id: str,
    reduce_id: int,
    num_map: int,
    output_path: str,
    tmp_suffix: str,
) -> None:
    """Read this reduce task's intermediate file from every map task, group
    the pairs by key, call reduce_fn once per key, and write one output file."""
    job = load_job(job_source_path(shared_root, job_id))

    pairs = []
    for map_id in range(num_map):
        with open(intermediate_path(shared_root, job_id, map_id, reduce_id), encoding="utf-8") as f:
            for line in f:
                pairs.append(json.loads(line))
    pairs.sort(key=itemgetter(0))

    final = output_part_path(shared_root, output_path, reduce_id)
    final.parent.mkdir(parents=True, exist_ok=True)
    # The temporary file lives in the job's own directory, not in the output
    # directory, so a killed worker never leaves a stray file in the user's
    # output. Both are in shared storage, so the rename is still atomic.
    temp = intermediate_dir(shared_root, job_id) / f"reduce-{reduce_id}.out{tmp_suffix}"
    try:
        with open(temp, "w", encoding="utf-8") as out:
            for key, group in groupby(pairs, key=itemgetter(0)):
                values = (value for _, value in group)
                for out_key, out_value in job.reduce_fn(key, values) or ():
                    out.write(f"{out_key}\t{out_value}\n")
        os.replace(temp, final)
    except BaseException:
        temp.unlink(missing_ok=True)
        raise


def run_task(shared_root: str, task: dict) -> None:
    """Run one task described by a plain dict (see mapreduce.worker)."""
    root = Path(shared_root)
    tmp_suffix = f".tmp-{task['worker_id']}-{task['attempt']}"
    if task["kind"] == "map":
        chunks = [Chunk(path, start, end) for path, start, end in task["chunks"]]
        run_map(root, task["job_id"], task["task_id"], chunks, task["num_reduce"], tmp_suffix)
    elif task["kind"] == "reduce":
        run_reduce(
            root, task["job_id"], task["task_id"], task["num_map"], task["output_path"], tmp_suffix
        )
    else:
        raise ValueError(f"unknown task kind: {task['kind']!r}")


def child_main(shared_root: str, task: dict, result) -> None:
    """Entry point of the process a worker starts to run one task.

    Sends None through the `result` pipe if the task succeeded, or the text
    of the traceback if it raised.
    """
    try:
        run_task(shared_root, task)
    except BaseException:
        result.send(traceback.format_exc()[-8000:])
    else:
        result.send(None)
    finally:
        result.close()
