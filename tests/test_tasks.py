"""Map and reduce task code, run directly in the test process."""

import os
import subprocess
import sys

import pytest

from helpers import (
    REPO,
    TEXTS,
    WORDCOUNT,
    install_job,
    map_task,
    read_output,
    reference_wordcount,
    run_job_locally,
    write_input,
)
from mapreduce.splits import list_input_files, make_splits
from mapreduce.storage import intermediate_dir
from mapreduce.tasks import JobError, load_job, partition, run_task


def job_file(tmp_path, source: str):
    path = tmp_path / "job.py"
    path.write_text(source)
    return path


def all_files(root):
    return sorted(
        os.path.relpath(os.path.join(dirpath, name), root)
        for dirpath, _, names in os.walk(root)
        for name in names
    )


def test_partition_in_range_and_known_value():
    assert partition("hello", 4) == 2  # crc32(b"hello") == 907060870
    for num_reduce in (1, 2, 7):
        for key in ("", "a", "héllo", "the quick brown fox"):
            assert 0 <= partition(key, num_reduce) < num_reduce


def test_partition_same_in_another_process():
    other = subprocess.run(
        [sys.executable, "-c", "from mapreduce.tasks import partition; print(partition('some key', 7))"],
        cwd=REPO,
        env={**os.environ, "PYTHONHASHSEED": "12345"},
        capture_output=True,
        text=True,
        check=True,
    )
    assert int(other.stdout) == partition("some key", 7)


def test_wordcount_matches_reference(shared_root):
    write_input(shared_root, TEXTS)

    run_job_locally(shared_root, WORDCOUNT, "input/books", "output/wc", num_map=3, num_reduce=2)

    assert read_output(shared_root, "output/wc") == reference_wordcount(TEXTS)


def test_no_temp_files_left(shared_root):
    write_input(shared_root, TEXTS)

    run_job_locally(shared_root, WORDCOUNT, "input/books", "output/wc", num_map=3, num_reduce=2)

    assert [name for name in all_files(shared_root) if ".tmp-" in name] == []
    assert sorted(os.listdir(shared_root / "output/wc")) == ["part-00000.txt", "part-00001.txt"]


def test_failed_map_leaves_no_files(shared_root, tmp_path):
    write_input(shared_root, {"input/x.txt": "fine\nboom\n"})
    bad = job_file(
        tmp_path,
        "def map_fn(key, value):\n"
        "    if value == 'boom':\n"
        "        raise ValueError('map exploded')\n"
        "    yield value, 1\n"
        "def reduce_fn(key, values):\n"
        "    yield key, sum(values)\n",
    )
    install_job(shared_root, "job-test", bad)
    chunks = make_splits(list_input_files(shared_root, "input"), 1)[0]

    with pytest.raises(ValueError, match="map exploded"):
        run_task(str(shared_root), map_task("job-test", 0, chunks, 1, 2))

    assert all_files(intermediate_dir(shared_root, "job-test")) == []


def test_failed_reduce_leaves_no_output_files(shared_root, tmp_path):
    write_input(shared_root, {"input/x.txt": "a\nb\n"})
    bad = job_file(
        tmp_path,
        "def map_fn(key, value):\n"
        "    yield value, 1\n"
        "def reduce_fn(key, values):\n"
        "    raise ValueError('reduce exploded')\n",
    )

    with pytest.raises(ValueError, match="reduce exploded"):
        run_job_locally(shared_root, bad, "input", "output/x", num_map=1, num_reduce=1)

    assert all_files(shared_root / "output") == []


def test_non_string_key_raises(shared_root, tmp_path):
    write_input(shared_root, {"input/x.txt": "a\n"})
    bad = job_file(
        tmp_path,
        "def map_fn(key, value):\n"
        "    yield 42, 1\n"
        "def reduce_fn(key, values):\n"
        "    yield key, sum(values)\n",
    )

    with pytest.raises(TypeError, match="must be strings"):
        run_job_locally(shared_root, bad, "input", "output/x", num_map=1, num_reduce=1)


def test_functions_returning_none_mean_no_output(shared_root, tmp_path):
    write_input(shared_root, {"input/x.txt": "a\nb\n"})
    quiet = job_file(
        tmp_path,
        "def map_fn(key, value):\n"
        "    return None\n"
        "def reduce_fn(key, values):\n"
        "    return None\n",
    )

    run_job_locally(shared_root, quiet, "input", "output/x", num_map=2, num_reduce=2)

    assert read_output(shared_root, "output/x") == {}
    assert sorted(os.listdir(shared_root / "output/x")) == ["part-00000.txt", "part-00001.txt"]


def test_load_job_missing_function(tmp_path):
    only_map = job_file(tmp_path, "def map_fn(key, value):\n    yield value, 1\n")

    with pytest.raises(JobError, match="reduce_fn"):
        load_job(only_map)


def test_empty_partition_writes_empty_part_file(shared_root):
    write_input(shared_root, {"input/x.txt": "same same same\n"})

    run_job_locally(shared_root, WORDCOUNT, "input", "output/x", num_map=1, num_reduce=4)

    sizes = sorted((shared_root / "output/x" / f"part-{r:05d}.txt").stat().st_size for r in range(4))
    assert sizes[:3] == [0, 0, 0]
    assert read_output(shared_root, "output/x") == {"same": "3"}


def test_output_sorted_by_key(shared_root):
    write_input(shared_root, TEXTS)

    run_job_locally(shared_root, WORDCOUNT, "input/books", "output/wc", num_map=2, num_reduce=1)

    lines = (shared_root / "output/wc/part-00000.txt").read_text().splitlines()
    keys = [line.split("\t")[0] for line in lines]
    assert keys == sorted(keys)
    assert len(keys) == len(reference_wordcount(TEXTS))


def test_map_fn_receives_file_path_and_line(shared_root, tmp_path):
    write_input(shared_root, {"input/a.txt": "one\ntwo\n", "input/b.txt": "three\n"})
    lines_per_file = job_file(
        tmp_path,
        "def map_fn(key, value):\n"
        "    yield key, value\n"
        "def reduce_fn(key, values):\n"
        "    yield key, ','.join(sorted(values))\n",
    )

    run_job_locally(shared_root, lines_per_file, "input", "output/x", num_map=2, num_reduce=2)

    assert read_output(shared_root, "output/x") == {"input/a.txt": "one,two", "input/b.txt": "three"}


def test_values_keep_their_types(shared_root, tmp_path):
    write_input(shared_root, {"input/x.txt": "abc\nde\n"})
    structured = job_file(
        tmp_path,
        "def map_fn(key, value):\n"
        "    yield 'stats', {'chars': len(value), 'lines': 1}\n"
        "def reduce_fn(key, values):\n"
        "    values = list(values)\n"
        "    yield 'chars', sum(v['chars'] for v in values)\n"
        "    yield 'lines', sum(v['lines'] for v in values)\n",
    )

    run_job_locally(shared_root, structured, "input", "output/x", num_map=1, num_reduce=1)

    assert read_output(shared_root, "output/x") == {"chars": "5", "lines": "2"}


# Combiners.


def test_combiner_gives_the_same_output(shared_root):
    from helpers import WORDCOUNT_COMBINER

    write_input(shared_root, TEXTS)

    run_job_locally(shared_root, WORDCOUNT_COMBINER, "input/books", "output/wc", num_map=3, num_reduce=2)

    assert read_output(shared_root, "output/wc") == reference_wordcount(TEXTS)


def test_combiner_writes_one_pair_per_distinct_key(shared_root):
    from helpers import WORDCOUNT_COMBINER, run_map_phase

    write_input(shared_root, {"input/x.txt": "a b a a c b a\n"})

    plain = run_map_phase(shared_root, WORDCOUNT, "input", num_map=1, num_reduce=2, job_id="plain")
    combined = run_map_phase(shared_root, WORDCOUNT_COMBINER, "input", num_map=1, num_reduce=2, job_id="combined")

    assert len(plain) == 7  # one pair per word
    assert sorted(combined) == [("a", 4), ("b", 2), ("c", 1)]


def test_combiner_works_in_small_batches(shared_root, monkeypatch):
    from helpers import WORDCOUNT_COMBINER, run_map_phase
    from mapreduce import tasks

    monkeypatch.setattr(tasks, "COMBINE_BATCH", 3)  # combine after every 3 pairs
    write_input(shared_root, {"input/x.txt": "a b a a c b a\n"})

    combined = run_map_phase(shared_root, WORDCOUNT_COMBINER, "input", num_map=1, num_reduce=1, job_id="batched")
    run_job_locally(shared_root, WORDCOUNT_COMBINER, "input", "output/x", num_map=1, num_reduce=1)

    assert len(combined) > 3  # "a" was combined in more than one batch
    assert read_output(shared_root, "output/x") == {"a": "4", "b": "2", "c": "1"}


def test_combiner_that_changes_keys_is_partitioned_by_the_new_key(shared_root, tmp_path):
    write_input(shared_root, {"input/x.txt": "aa bb c dd e fff\n", "input/y.txt": "gg h iii\n"})
    by_length = job_file(
        tmp_path,
        "def map_fn(key, value):\n"
        "    for word in value.split():\n"
        "        yield word, 1\n"
        "def combine_fn(key, values):\n"
        "    yield 'length ' + str(len(key)), sum(values)\n"
        "def reduce_fn(key, values):\n"
        "    yield key, sum(values)\n",
    )

    run_job_locally(shared_root, by_length, "input", "output/x", num_map=2, num_reduce=3)

    # read_output fails if a key shows up in two part files, which is what
    # would happen if pairs were partitioned by the key from before combining.
    assert read_output(shared_root, "output/x") == {"length 1": "3", "length 2": "4", "length 3": "2"}


def test_combiner_yielding_a_non_string_key_raises(shared_root, tmp_path):
    write_input(shared_root, {"input/x.txt": "a\n"})
    bad = job_file(
        tmp_path,
        "def map_fn(key, value):\n"
        "    yield value, 1\n"
        "def combine_fn(key, values):\n"
        "    yield 7, sum(values)\n"
        "def reduce_fn(key, values):\n"
        "    yield key, sum(values)\n",
    )

    with pytest.raises(TypeError, match="combine_fn"):
        run_job_locally(shared_root, bad, "input", "output/x", num_map=1, num_reduce=1)


def test_combine_fn_must_be_a_function(tmp_path):
    bad = job_file(
        tmp_path,
        "def map_fn(key, value):\n    yield value, 1\n"
        "def reduce_fn(key, values):\n    yield key, sum(values)\n"
        "combine_fn = 5\n",
    )

    with pytest.raises(JobError, match="combine_fn"):
        load_job(bad)


def test_combiner_that_is_not_the_same_function_as_the_reducer(shared_root, tmp_path, monkeypatch):
    """An average: the combiner passes on a sum and a count, and only the
    reducer divides. Reusing the reducer as the combiner would be wrong."""
    from mapreduce import tasks

    monkeypatch.setattr(tasks, "COMBINE_BATCH", 2)  # so a key is combined in several batches
    write_input(shared_root, {"input/a.txt": "aa\nbbbb\ncccccc\n", "input/b.txt": "d\nddd\n"})
    average_line_length = job_file(
        tmp_path,
        "def map_fn(key, value):\n"
        "    yield key, [len(value), 1]\n"
        "def combine_fn(key, values):\n"
        "    values = list(values)\n"
        "    yield key, [sum(v[0] for v in values), sum(v[1] for v in values)]\n"
        "def reduce_fn(key, values):\n"
        "    values = list(values)\n"
        "    yield key, sum(v[0] for v in values) / sum(v[1] for v in values)\n",
    )

    run_job_locally(shared_root, average_line_length, "input", "output/x", num_map=2, num_reduce=2)

    assert read_output(shared_root, "output/x") == {"input/a.txt": "4.0", "input/b.txt": "2.0"}
