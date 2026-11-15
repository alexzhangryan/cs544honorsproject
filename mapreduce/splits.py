"""Splitting input files into byte ranges, and reading the lines of a range.

The boss cuts the input into one list of chunks per map task. A chunk is a
byte range of one file. A line belongs to the chunk that contains its first
byte, so every line is read by exactly one map task even when a chunk
boundary falls in the middle of a line.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator


@dataclass(frozen=True)
class Chunk:
    """Bytes [start, end) of one input file. The path is relative to the shared root."""

    path: str
    start: int
    end: int


def list_input_files(shared_root: Path, input_path: str) -> list[tuple[str, int]]:
    """Return (relative path, size) for the input file, or for every file under
    the input directory. Hidden files and directories are skipped."""
    top = shared_root / input_path
    if top.is_file():
        return [(Path(input_path).as_posix(), top.stat().st_size)]
    if not top.is_dir():
        raise FileNotFoundError(f"input path not found: {input_path}")

    files = []
    for dirpath, dirnames, filenames in os.walk(top):
        dirnames[:] = [name for name in dirnames if not name.startswith(".")]
        for name in filenames:
            full = Path(dirpath) / name
            if not name.startswith(".") and full.is_file():
                files.append((full.relative_to(shared_root).as_posix(), full.stat().st_size))
    return sorted(files)


def make_splits(files: list[tuple[str, int]], num_map: int) -> list[list[Chunk]]:
    """Cut the files into exactly num_map lists of chunks of nearly equal size.

    The files are treated as one long run of bytes, cut at evenly spaced
    positions. A split can therefore cover part of a file, several files, or
    nothing at all when there are more map tasks than bytes.
    """
    total = sum(size for _, size in files)
    splits = []
    for i in range(num_map):
        low = total * i // num_map
        high = total * (i + 1) // num_map
        chunks = []
        file_start = 0  # position of the current file in the long run of bytes
        for path, size in files:
            start = max(low, file_start) - file_start
            end = min(high, file_start + size) - file_start
            if start < end:
                chunks.append(Chunk(path, start, end))
            file_start += size
        splits.append(chunks)
    return splits


def read_lines(shared_root: Path, chunk: Chunk) -> Iterator[str]:
    """Yield every line whose first byte lies in the chunk, without its line ending."""
    with open(shared_root / chunk.path, "rb", buffering=1024 * 1024) as f:
        if chunk.start > 0:
            # Step back one byte and read to the end of that line. If the
            # previous byte is a newline, this consumes only the newline and
            # the line starting at chunk.start is ours. Otherwise we were in
            # the middle of a line that belongs to the previous chunk.
            f.seek(chunk.start - 1)
            f.readline()
        position = f.tell()
        while position < chunk.end:
            line = f.readline()
            if not line:
                break
            position += len(line)
            if line.endswith(b"\n"):
                line = line[:-1]
            if line.endswith(b"\r"):
                line = line[:-1]
            yield line.decode("utf-8", errors="replace")
