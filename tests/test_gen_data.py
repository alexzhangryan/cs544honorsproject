"""The dataset generator used for the sample data and the benchmarks."""

import subprocess
import sys

from helpers import REPO

SCRIPT = REPO / "scripts" / "gen_data.py"


def generate(out, seed=7, size_mb="0.2", files="3"):
    subprocess.run(
        [sys.executable, str(SCRIPT), "--out", str(out), "--size-mb", size_mb, "--files", files, "--seed", str(seed)],
        check=True,
        capture_output=True,
    )
    return {path.name: path.read_bytes() for path in sorted(out.iterdir())}


def test_generates_requested_files_and_size(tmp_path):
    data = generate(tmp_path / "a")

    assert sorted(data) == ["part-00.txt", "part-01.txt", "part-02.txt"]
    total = sum(len(content) for content in data.values())
    assert 0.2 * 1024 * 1024 <= total < 0.2 * 1024 * 1024 * 1.1


def test_content_is_lines_of_lowercase_words(tmp_path):
    data = generate(tmp_path / "a")

    text = data["part-00.txt"].decode()
    assert text.endswith("\n")
    lines = text.splitlines()
    assert all(len(line.split()) == 10 for line in lines)
    assert all(word.isalpha() and word.islower() for word in lines[0].split())
    assert len(set(text.split())) > 100  # a real vocabulary, not a handful of words


def test_same_seed_same_data_and_different_seed_differs(tmp_path):
    assert generate(tmp_path / "a", seed=7) == generate(tmp_path / "b", seed=7)
    assert generate(tmp_path / "c", seed=8) != generate(tmp_path / "a", seed=7)
