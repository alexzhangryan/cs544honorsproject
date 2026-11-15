"""The command-line client. It runs on the host, outside any container.

    python -m mapreduce.client upload data/sample input/sample
    python -m mapreduce.client submit --input input/sample --output output/wordcount \\
        --job examples/wordcount.py --maps 8 --reduces 4
    python -m mapreduce.client status <job-id>
    python -m mapreduce.client workers

Input and output paths are relative to shared storage. Uploading is a plain
file copy into the shared directory; everything else is a gRPC call to the
boss.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Callable, Optional

import grpc

from mapreduce import MAX_TASKS
from mapreduce import mapreduce_pb2 as pb
from mapreduce import mapreduce_pb2_grpc as pb_grpc
from mapreduce.storage import resolve_inside

RPC_TIMEOUT = 10.0  # seconds to wait for the boss to answer one call


class ClientError(Exception):
    """A problem that is shown to the user as a single line."""


def first_line(error: Exception) -> str:
    """The first line of an error's message (some span several lines)."""
    return (str(error).splitlines() or [type(error).__name__])[0]


def upload(local_path: Path, shared_dir: Path, dest: str) -> Path:
    """Copy a local file or directory to `dest` inside shared storage."""
    if not shared_dir.is_dir():
        raise ClientError(
            f"shared storage directory not found: {shared_dir} "
            "(run this from the repository root, or pass --shared)"
        )
    if not local_path.exists():
        raise ClientError(f"no such file or directory: {local_path}")
    try:
        target = resolve_inside(shared_dir, dest)
    except ValueError as error:
        raise ClientError(str(error))
    source = local_path.resolve()
    if source == target or source in target.parents:
        # Copying would descend into its own copy without end.
        raise ClientError(f"cannot upload {local_path} into itself: it contains shared storage")

    try:
        if local_path.is_dir():
            shutil.copytree(local_path, target, dirs_exist_ok=True)
        else:
            if target.is_dir():
                target = target / local_path.name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(local_path, target)
    except OSError as error:
        raise ClientError(f"cannot copy {local_path} into shared storage: {first_line(error)}")
    return target


def submit_job(
    stub,
    input_path: str,
    output_path: str,
    job_file: Path,
    maps: int,
    reduces: int,
    reduce_order: str = "largest_first",
) -> str:
    """Send a job to the boss and return its id."""
    for option, count in (("--maps", maps), ("--reduces", reduces)):
        if not 1 <= count <= MAX_TASKS:
            raise ClientError(f"{option} must be between 1 and {MAX_TASKS}")
    try:
        source = Path(job_file).read_text(encoding="utf-8")
    except FileNotFoundError:
        raise ClientError(f"job file not found: {job_file}")
    except (OSError, UnicodeDecodeError) as error:
        raise ClientError(f"cannot read the job file {job_file}: {first_line(error)}")
    request = pb.SubmitJobRequest(
        input_path=input_path,
        output_path=output_path,
        job_source=source,
        num_map=maps,
        num_reduce=reduces,
        reduce_order=reduce_order,
    )
    return stub.SubmitJob(request, timeout=RPC_TIMEOUT).job_id


def wait_for_job(stub, job_id: str, poll: float = 0.5, on_progress: Optional[Callable] = None):
    """Poll until the job has succeeded or failed, and return its final status."""
    while True:
        status = stub.GetJobStatus(pb.JobStatusRequest(job_id=job_id), timeout=RPC_TIMEOUT)
        if on_progress is not None:
            on_progress(status)
        if status.state != "RUNNING":
            return status
        time.sleep(poll)


def progress_line(status) -> str:
    return (
        f"map {status.maps_done}/{status.maps_total}  "
        f"reduce {status.reduces_done}/{status.reduces_total}  "
        f"{status.elapsed_seconds:.1f}s"
    )


def format_size(size: int) -> str:
    for unit in ("bytes", "KB", "MB"):
        if size < 1024:
            return f"{size:.0f} {unit}" if unit == "bytes" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} GB"


def requeue_note(status) -> str:
    if status.requeues == 0:
        return ""
    tasks = "task" if status.requeues == 1 else "tasks"
    return f"{status.requeues} {tasks} re-run after a worker failure"


def cmd_upload(args, stub) -> int:
    target = upload(Path(args.local), Path(args.shared), args.dest)
    count = sum(1 for path in target.rglob("*") if path.is_file()) if target.is_dir() else 1
    files = "file" if count == 1 else "files"
    print(f"uploaded {args.local} to {Path(args.shared) / args.dest} ({count} {files})")
    return 0


def cmd_submit(args, stub) -> int:
    job_id = submit_job(
        stub, args.input, args.output, Path(args.job), args.maps, args.reduces,
        reduce_order=args.reduce_order.replace("-", "_"),
    )
    print(f"submitted {job_id}")
    if args.no_wait:
        return 0

    last_counts = None

    def show_progress(status) -> None:
        nonlocal last_counts
        counts = (status.maps_done, status.reduces_done)
        if counts != last_counts:
            print(progress_line(status), flush=True)
            last_counts = counts

    try:
        status = wait_for_job(stub, job_id, on_progress=show_progress)
    except KeyboardInterrupt:
        print(f"\nstopped waiting; the job is still running (check it with: status {job_id})")
        return 130

    if status.state == "SUCCEEDED":
        note = requeue_note(status)
        print(f"SUCCEEDED in {status.elapsed_seconds:.1f}s" + (f" ({note})" if note else ""))
        print(f"intermediate data: {format_size(status.intermediate_bytes)}")
        print(f"output: {Path(args.shared) / status.output_path}")
        return 0
    print(f"FAILED after {status.elapsed_seconds:.1f}s")
    print(status.error)
    return 1


def cmd_status(args, stub) -> int:
    status = stub.GetJobStatus(pb.JobStatusRequest(job_id=args.job_id), timeout=RPC_TIMEOUT)
    print(f"{status.job_id}  {status.state}  {progress_line(status)}")
    if requeue_note(status):
        print(requeue_note(status))
    if status.intermediate_bytes:
        print(f"intermediate data: {format_size(status.intermediate_bytes)}")
    if status.state == "FAILED":
        print(status.error)
    return 0


def cmd_workers(args, stub) -> int:
    workers = stub.ListWorkers(pb.ListWorkersRequest(), timeout=RPC_TIMEOUT).workers
    if not workers:
        print("no workers have contacted the boss yet")
        return 0
    width = max(len(worker.worker_id) for worker in workers)
    for worker in workers:
        if worker.alive:
            detail = worker.current_task or "idle"
        else:
            detail = f"last contact {worker.seconds_since_contact:.1f}s ago"
        print(f"{worker.worker_id:<{width}}  {'alive' if worker.alive else 'DEAD '}  {detail}")
    return 0


def describe_rpc_error(error: grpc.RpcError, boss: str) -> str:
    code = error.code()
    if code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
        return f"cannot reach the boss at {boss} (is the cluster running? try: docker compose up -d)"
    if code in (grpc.StatusCode.INVALID_ARGUMENT, grpc.StatusCode.NOT_FOUND):
        return error.details()
    return f"{code.name}: {error.details()}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m mapreduce.client",
        description="Upload data, submit MapReduce jobs and check on them.",
    )
    parser.add_argument("--boss", default=os.environ.get("MR_BOSS", "localhost:50051"),
                        help="address of the boss (default: $MR_BOSS or localhost:50051)")
    parser.add_argument("--shared", default=os.environ.get("MR_SHARED_DIR", "shared"),
                        help="shared storage directory on this machine "
                             "(default: $MR_SHARED_DIR or ./shared)")
    commands = parser.add_subparsers(dest="command", required=True)

    upload_cmd = commands.add_parser("upload", help="copy a local file or directory into shared storage")
    upload_cmd.add_argument("local", help="local file or directory")
    upload_cmd.add_argument("dest", help="destination, relative to shared storage (for example input/sample)")
    upload_cmd.set_defaults(run=cmd_upload)

    submit_cmd = commands.add_parser("submit", help="submit a job and wait for it to finish")
    submit_cmd.add_argument("--input", required=True, help="input file or directory, relative to shared storage")
    submit_cmd.add_argument("--output", required=True, help="output directory, relative to shared storage")
    submit_cmd.add_argument("--job", required=True, help="local Python file defining map_fn and reduce_fn")
    submit_cmd.add_argument("--maps", required=True, type=int, help="number of map tasks")
    submit_cmd.add_argument("--reduces", required=True, type=int, help="number of reduce tasks")
    submit_cmd.add_argument("--no-wait", action="store_true", help="print the job id and return immediately")
    submit_cmd.add_argument("--reduce-order", choices=["largest-first", "numerical"], default="largest-first",
                            help="hand out reduce tasks with the most input first (the default), "
                                 "or in order of task number")
    submit_cmd.set_defaults(run=cmd_submit)

    status_cmd = commands.add_parser("status", help="show the progress of a job")
    status_cmd.add_argument("job_id")
    status_cmd.set_defaults(run=cmd_status)

    workers_cmd = commands.add_parser("workers", help="list the workers and whether each is alive")
    workers_cmd.set_defaults(run=cmd_workers)

    return parser


def main(argv: Optional[list] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        with grpc.insecure_channel(args.boss) as channel:
            return args.run(args, pb_grpc.BossStub(channel))
    except ClientError as error:
        print(f"error: {error}", file=sys.stderr)
    except grpc.RpcError as error:
        print(f"error: {describe_rpc_error(error, args.boss)}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
