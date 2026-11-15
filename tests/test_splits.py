"""Input splitting: every byte and every line is covered exactly once."""

from collections import Counter
from pathlib import Path

import pytest

from mapreduce.splits import Chunk, list_input_files, make_splits, read_lines

FILES = {
    "in/a.txt": b"alpha\nbeta\ngamma\n",
    "in/b.txt": b"no trailing newline\nlast line",
    "in/blank.txt": b"\n\nafter blank lines\n",
    "in/empty.txt": b"",
    "in/long.txt": b"x" * 500 + b"\nshort\n",
}


def write(root: Path, rel: str, data: bytes) -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def read_all(root: Path, splits: list) -> list:
    """Every (path, line) pair produced by reading every chunk of every split."""
    return [
        (chunk.path, line)
        for chunks in splits
        for chunk in chunks
        for line in read_lines(root, chunk)
    ]


@pytest.mark.parametrize("num_map", [1, 3, 50])
def test_make_splits_returns_exactly_num_map_lists(num_map):
    assert len(make_splits([("a.txt", 10)], num_map)) == num_map


@pytest.mark.parametrize("sizes", [[10], [0, 7, 0], [100, 1, 37], [5, 5, 5, 5]])
def test_make_splits_tiles_every_byte_once(sizes):
    files = [(f"f{i}.txt", size) for i, size in enumerate(sizes)]
    for num_map in range(1, 21):
        by_path = {path: [] for path, _ in files}
        for chunks in make_splits(files, num_map):
            for chunk in chunks:
                assert chunk.start < chunk.end
                by_path[chunk.path].append(chunk)
        for path, size in files:
            position = 0
            for chunk in sorted(by_path[path], key=lambda c: c.start):
                assert chunk.start == position
                position = chunk.end
            assert position == size


def test_make_splits_are_balanced():
    splits = make_splits([("a.txt", 100), ("b.txt", 3)], 7)
    sizes = [sum(c.end - c.start for c in chunks) for chunks in splits]
    assert sum(sizes) == 103
    assert max(sizes) - min(sizes) <= 1


@pytest.mark.parametrize("num_map", [1, 2, 3, 7, 50])
def test_every_line_read_exactly_once(tmp_path, num_map):
    for rel, data in FILES.items():
        write(tmp_path, rel, data)
    expected = Counter(
        (rel, line) for rel, data in FILES.items() for line in data.decode().splitlines()
    )

    splits = make_splits(list_input_files(tmp_path, "in"), num_map)

    assert Counter(read_all(tmp_path, splits)) == expected


def test_more_map_tasks_than_bytes(tmp_path):
    write(tmp_path, "in/tiny.txt", b"a\nb\n")

    splits = make_splits(list_input_files(tmp_path, "in"), 10)

    assert len(splits) == 10
    assert sorted(line for _, line in read_all(tmp_path, splits)) == ["a", "b"]


def test_crlf_and_invalid_utf8(tmp_path):
    write(tmp_path, "in/odd.txt", b"a\r\nb\xff\n")

    lines = list(read_lines(tmp_path, Chunk("in/odd.txt", 0, 7)))

    assert lines == ["a", "b�"]


def test_list_input_files_skips_hidden_and_recurses(tmp_path):
    write(tmp_path, "in/b.txt", b"bb")
    write(tmp_path, "in/a.txt", b"a")
    write(tmp_path, "in/sub/x.txt", b"xxx")
    write(tmp_path, "in/.DS_Store", b"junk")
    write(tmp_path, "in/.hidden/y.txt", b"junk")

    assert list_input_files(tmp_path, "in") == [
        ("in/a.txt", 1),
        ("in/b.txt", 2),
        ("in/sub/x.txt", 3),
    ]


def test_list_input_files_single_file(tmp_path):
    write(tmp_path, "in/a.txt", b"hello")

    assert list_input_files(tmp_path, "in/a.txt") == [("in/a.txt", 5)]


def test_list_input_files_missing_raises(tmp_path):
    with pytest.raises(FileNotFoundError, match="in/nope"):
        list_input_files(tmp_path, "in/nope")
