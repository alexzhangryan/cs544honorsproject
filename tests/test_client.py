"""The command-line client, run in-process against a real cluster."""

import time

from helpers import TEXTS, WORDCOUNT, read_output, reference_wordcount, write_input
from mapreduce.client import main


def client(cluster, shared_root, *args):
    return main(["--boss", f"127.0.0.1:{cluster.port}", "--shared", str(shared_root), *args])


def submit_args(job=WORDCOUNT, output="output/wc"):
    return ["submit", "--input", "input/books", "--output", output, "--job", str(job),
            "--maps", "3", "--reduces", "2"]


def assert_one_line_error(captured, fragment):
    assert captured.err.startswith("error: ")
    assert captured.err.count("\n") == 1
    assert fragment in captured.err
    assert "Traceback" not in captured.err + captured.out


def test_upload_directory_and_file(shared_root, tmp_path, capsys):
    local = tmp_path / "local"
    (local / "sub").mkdir(parents=True)
    (local / "a.txt").write_text("alpha\n")
    (local / "sub" / "b.txt").write_text("beta\n")

    assert main(["--shared", str(shared_root), "upload", str(local), "input/data"]) == 0
    assert (shared_root / "input/data/a.txt").read_text() == "alpha\n"
    assert (shared_root / "input/data/sub/b.txt").read_text() == "beta\n"
    assert "input/data" in capsys.readouterr().out

    assert main(["--shared", str(shared_root), "upload", str(local / "a.txt"), "input/single.txt"]) == 0
    assert (shared_root / "input/single.txt").read_text() == "alpha\n"


def test_upload_rejects_escaping_dest(shared_root, tmp_path, capsys):
    local = tmp_path / "a.txt"
    local.write_text("alpha\n")

    assert main(["--shared", str(shared_root), "upload", str(local), "../outside.txt"]) == 1

    assert_one_line_error(capsys.readouterr(), "inside shared storage")
    assert not (shared_root.parent / "outside.txt").exists()


def test_upload_missing_source(shared_root, tmp_path, capsys):
    assert main(["--shared", str(shared_root), "upload", str(tmp_path / "nope"), "input/x"]) == 1

    assert_one_line_error(capsys.readouterr(), "nope")


def test_upload_missing_shared_dir(tmp_path, capsys):
    local = tmp_path / "a.txt"
    local.write_text("alpha\n")

    assert main(["--shared", str(tmp_path / "no-shared"), "upload", str(local), "input/a.txt"]) == 1

    assert_one_line_error(capsys.readouterr(), "shared storage directory not found")


def test_submit_waits_and_reports_success(cluster, shared_root, capsys):
    write_input(shared_root, TEXTS)
    cluster.start_workers(2)

    assert client(cluster, shared_root, *submit_args()) == 0

    out = capsys.readouterr().out
    assert "submitted job-" in out
    assert "map 3/3  reduce 2/2" in out
    assert "SUCCEEDED" in out
    assert "output/wc" in out
    assert read_output(shared_root, "output/wc") == reference_wordcount(TEXTS)


def test_submit_no_wait_returns_immediately(cluster, shared_root, capsys):
    write_input(shared_root, TEXTS)  # no workers, so the job cannot finish

    started = time.monotonic()
    assert client(cluster, shared_root, *submit_args(), "--no-wait") == 0

    assert time.monotonic() - started < 5
    job_id = capsys.readouterr().out.split()[1]
    assert cluster.status(job_id).state == "RUNNING"


def test_submit_bad_output_dir_exits_1_with_one_line_error(cluster, shared_root, capsys):
    write_input(shared_root, TEXTS)
    write_input(shared_root, {"output/wc/part-00000.txt": "old\t1\n"})

    assert client(cluster, shared_root, *submit_args()) == 1

    assert_one_line_error(capsys.readouterr(), "already exists")


def test_submit_missing_job_file(cluster, shared_root, tmp_path, capsys):
    write_input(shared_root, TEXTS)

    assert client(cluster, shared_root, *submit_args(job=tmp_path / "nope.py")) == 1

    assert_one_line_error(capsys.readouterr(), "job file not found")


def test_failed_job_exits_1_and_prints_error(cluster, shared_root, tmp_path, capsys):
    write_input(shared_root, TEXTS)
    bad = tmp_path / "bad.py"
    bad.write_text(
        "def map_fn(key, value):\n"
        "    raise ValueError('bad line')\n"
        "def reduce_fn(key, values):\n"
        "    yield key, sum(values)\n"
    )
    cluster.start_workers(1)

    assert client(cluster, shared_root, *submit_args(job=bad)) == 1

    out = capsys.readouterr().out
    assert "FAILED" in out
    assert "ValueError: bad line" in out


def test_status_command(cluster, shared_root, capsys):
    write_input(shared_root, TEXTS)
    cluster.start_workers(1)
    job_id = cluster.submit(WORDCOUNT, "input/books", "output/wc", maps=3, reduces=2)
    cluster.wait(job_id)

    assert client(cluster, shared_root, "status", job_id) == 0
    out = capsys.readouterr().out
    assert job_id in out
    assert "SUCCEEDED" in out
    assert "map 3/3  reduce 2/2" in out

    assert client(cluster, shared_root, "status", "job-nope") == 1
    assert_one_line_error(capsys.readouterr(), "unknown job")


def test_boss_unreachable_message(shared_root, capsys):
    started = time.monotonic()

    assert main(["--boss", "127.0.0.1:1", "--shared", str(shared_root), "status", "job-x"]) == 1

    assert time.monotonic() - started < 10
    assert_one_line_error(capsys.readouterr(), "cannot reach the boss at 127.0.0.1:1")
