"""Run the performance experiments on the real Docker Compose cluster.

    python bench/run_benchmarks.py [--size-mb 100] [--trials 3] [--only NAME]

Three experiments, all running word count with 16 map and 8 reduce tasks:

  scaling   job time with 1, 2, 3 and 4 live workers
  failure   job time with no failure, with a worker killed during the map
            phase, with a worker killed during the reduce phase, and with
            only three workers from the start
  timeline  one run with a worker killed during the map phase, keeping the
            boss's scheduling log so the recovery can be drawn

and one for each of the additional features:

  combiner      word count with and without combine_fn
  reduce_order  reduce tasks handed out in numerical order and largest first
  backup        one worker limited to a quarter of a CPU, with backup tasks
                on and off, plus one such run kept for drawing

The first three experiments run with the additional features switched off
(numerical reduce order, no backup tasks, no combiner), so they measure the
base system. Each of the other three compares its own feature switched on and
off. There is no combiner in reduce_order and backup, and no backup tasks in
combiner and reduce_order; combiner and backup use largest-first reduce
order, the client's default.

Results are written to bench/results/. Draw the plots with bench/plot.py.

This script starts, stops and kills the worker containers. It needs the
client set up as described in the README, and it generates a dataset in
data/bench if there is none.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

# This script runs docker commands from a process that also uses gRPC.
# Without this, gRPC logs a notice about fork handlers each time.
os.environ.setdefault("GRPC_ENABLE_FORK_SUPPORT", "0")

import grpc  # noqa: E402

from mapreduce import mapreduce_pb2 as pb  # noqa: E402
from mapreduce import mapreduce_pb2_grpc as pb_grpc  # noqa: E402
from mapreduce.client import submit_job, upload, wait_for_job  # noqa: E402
from mapreduce.storage import events_path, job_dir  # noqa: E402

SHARED = REPO / "shared"
RESULTS = REPO / "bench" / "results"
DATA = REPO / "data" / "bench"
WORDCOUNT = REPO / "examples" / "wordcount.py"
WORDCOUNT_COMBINER = REPO / "examples" / "wordcount_combiner.py"
INPUT = "input/bench"
MAPS = 16
REDUCES = 8
ALL_WORKERS = [1, 2, 3, 4]
VICTIM = 2  # the worker that gets killed in the failure experiments
SLOW = 3  # the worker that is slowed down in the backup experiment
SLOW_CPUS = 0.25  # its CPU limit while slowed down
KILL_DELAY = 1.0  # seconds into the victim's task at which it is killed
COOLDOWN = 0.0  # seconds to pause before each run; set with --cooldown


def docker(*args: str) -> str:
    result = subprocess.run(["docker", *args], cwd=REPO, capture_output=True, text=True)
    if result.returncode != 0:
        raise SystemExit(f"docker {' '.join(args)} failed:\n{result.stderr}")
    return result.stdout.strip()


def wait_for(condition, what: str, timeout: float = 90, interval: float = 0.1) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(interval)
    raise SystemExit(f"timed out waiting until {what}")


def list_workers(stub) -> dict:
    reply = stub.ListWorkers(pb.ListWorkersRequest(), timeout=10)
    return {worker.worker_id: worker for worker in reply.workers}


def boss_is_up(stub) -> bool:
    try:
        list_workers(stub)
        return True
    except grpc.RpcError:
        return False


def set_live_workers(stub, live: list) -> None:
    """Start or stop worker containers so that exactly `live` are running,
    and wait until the boss agrees and every live worker is idle."""
    for n in ALL_WORKERS:
        name = f"mr-worker-{n}"
        running = docker("inspect", "--format", "{{.State.Running}}", name) == "true"
        if n in live and not running:
            docker("start", name)
        elif n not in live and running:
            docker("stop", "--time", "1", name)

    def settled() -> bool:
        workers = list_workers(stub)
        for n in ALL_WORKERS:
            worker = workers.get(f"worker-{n}")
            if n in live:
                if worker is None or not worker.alive or worker.current_task:
                    return False
            elif worker is not None and worker.alive:
                return False  # the boss has not noticed yet that it is gone
        return True

    wait_for(settled, f"the boss sees exactly workers {live} alive and idle")


def output_hash(output_dir: Path) -> str:
    """SHA-256 of the part files, concatenated in order."""
    digest = hashlib.sha256()
    for part in sorted(output_dir.glob("part-*.txt")):
        digest.update(part.read_bytes())
    return digest.hexdigest()


def kill_victim(stub, job_id: str, phase: str) -> float:
    """Kill the victim worker while it is running a task of the given phase.

    Returns the moment of the kill, in seconds since the job was submitted.
    """
    victim_id = f"worker-{VICTIM}"
    while True:
        status = stub.GetJobStatus(pb.JobStatusRequest(job_id=job_id), timeout=10)
        if status.state != "RUNNING":
            raise SystemExit("the job finished before a worker could be killed; use a larger --size-mb")
        task = list_workers(stub)[victim_id].current_task
        if phase == "map":
            ready = status.maps_done >= MAPS // 4 and task.startswith("map")
        else:
            ready = status.maps_done == status.maps_total and task.startswith("reduce")
        if not ready:
            time.sleep(0.05)
            continue

        # Let the victim get properly into the task, then make sure it is
        # still on the same one.
        time.sleep(KILL_DELAY)
        status = stub.GetJobStatus(pb.JobStatusRequest(job_id=job_id), timeout=10)
        asked_at = time.monotonic()
        if list_workers(stub)[victim_id].current_task != task:
            continue
        docker("kill", f"mr-worker-{VICTIM}")
        return status.elapsed_seconds + (time.monotonic() - asked_at)


def phase_times(events: list) -> dict:
    """Break a finished job's scheduling log down into its two phases."""
    map_done = next(event for event in events if event["event"] == "map_phase_done")
    times = {
        "map_seconds": round(map_done["t"], 2),
        "reduce_seconds": round(events[-1]["t"] - map_done["t"], 2),
        "largest_start": "",
    }
    sizes = map_done.get("reduce_sizes")
    if sizes:
        largest = sizes.index(max(sizes))
        started = next(
            event["t"] for event in events
            if event["event"] == "assigned" and (event["kind"], event["task"]) == ("reduce", largest)
        )
        # How long after the end of the map phase the largest reduce task was handed out.
        times["largest_start"] = round(started - map_done["t"], 2)
    return times


def run_job(
    stub,
    name: str,
    kill: str = "",
    job_file: Path = WORDCOUNT,
    reduce_order: str = "numerical",
    backup_tasks: bool = False,
) -> dict:
    """Run word count once and return its measurements. The boss's
    scheduling log for the run is kept in bench/results/events/.

    By default the additional features are off: numerical reduce order, no
    backup tasks, and a job file without a combiner.
    """
    output = f"output/bench-{name}"
    shutil.rmtree(SHARED / output, ignore_errors=True)
    time.sleep(COOLDOWN)
    job_id = submit_job(
        stub, INPUT, output, job_file, MAPS, REDUCES, reduce_order=reduce_order, backup_tasks=backup_tasks
    )
    kill_t = kill_victim(stub, job_id, kill) if kill else None
    status = wait_for_job(stub, job_id, poll=0.1)
    if status.state != "SUCCEEDED":
        raise SystemExit(f"job {job_id} failed:\n{status.error}")

    events = [json.loads(line) for line in events_path(SHARED, job_id).read_text().splitlines()]
    result = {
        "job_id": job_id,
        "seconds": round(status.elapsed_seconds, 2),
        "requeues": status.requeues,
        "backups": status.backup_tasks,
        "intermediate_bytes": status.intermediate_bytes,
        "sha256": output_hash(SHARED / output),
        "kill_t": kill_t,
        **phase_times(events),
    }
    shutil.copyfile(events_path(SHARED, job_id), RESULTS / "events" / f"{name}.jsonl")
    shutil.rmtree(SHARED / output, ignore_errors=True)
    shutil.rmtree(job_dir(SHARED, job_id), ignore_errors=True)
    print(f"  {name}: {result['seconds']}s, {result['requeues']} re-run", flush=True)
    return result


def open_csv(name: str, header: list):
    f = open(RESULTS / name, "w", newline="")
    writer = csv.writer(f)
    writer.writerow(header)
    return f, writer


def scaling_experiment(stub, trials: int) -> None:
    print("scaling: job time against the number of live workers", flush=True)
    f, writer = open_csv("scaling.csv", ["workers", "trial", "seconds"])
    with f:
        for trial in range(1, trials + 1):
            for count in (4, 3, 2, 1):
                set_live_workers(stub, ALL_WORKERS[:count])
                result = run_job(stub, f"scaling-{count}w-{trial}")
                writer.writerow([count, trial, result["seconds"]])
                f.flush()
    set_live_workers(stub, ALL_WORKERS)


def failure_experiment(stub, trials: int) -> None:
    print("failure: job time when a worker is killed", flush=True)
    survivors = [n for n in ALL_WORKERS if n != VICTIM]
    cases = [
        # name, live workers at the start, phase in which the victim is killed
        ("no_failure", ALL_WORKERS, ""),
        ("kill_during_map", ALL_WORKERS, "map"),
        ("kill_during_reduce", ALL_WORKERS, "reduce"),
        ("three_workers", survivors, ""),
    ]
    reference = None
    f, writer = open_csv("failure.csv", ["case", "trial", "seconds", "requeues", "output_sha256", "identical"])
    with f:
        for trial in range(1, trials + 1):
            # With four trials each case runs once in each position.
            for name, live, kill in rotated(cases, trial):
                set_live_workers(stub, live)
                result = run_job(stub, f"{name}-{trial}", kill=kill)
                if reference is None:
                    reference = result["sha256"]  # the first failure-free run
                writer.writerow([
                    name, trial, result["seconds"], result["requeues"], result["sha256"],
                    "yes" if result["sha256"] == reference else "NO",
                ])
                f.flush()
    set_live_workers(stub, ALL_WORKERS)


def timeline_experiment(stub) -> None:
    print("timeline: one recovery in detail", flush=True)
    set_live_workers(stub, ALL_WORKERS)
    result = run_job(stub, "timeline", kill="map")
    shutil.copyfile(RESULTS / "events" / "timeline.jsonl", RESULTS / "timeline_events.jsonl")
    meta = {
        "victim": f"worker-{VICTIM}",
        "kill_t": round(result["kill_t"], 2),
        "seconds": result["seconds"],
        "requeues": result["requeues"],
        "maps": MAPS,
        "reduces": REDUCES,
    }
    (RESULTS / "timeline_meta.json").write_text(json.dumps(meta, indent=2) + "\n")
    set_live_workers(stub, ALL_WORKERS)


def rotated(cases: list, trial: int) -> list:
    """The cases, starting with a different one in each trial, so that a
    machine that warms up within a trial affects every case equally."""
    shift = (trial - 1) % len(cases)
    return cases[shift:] + cases[:shift]


def combiner_experiment(stub, trials: int) -> None:
    print("combiner: word count with and without combine_fn", flush=True)
    cases = [("without_combiner", WORDCOUNT), ("with_combiner", WORDCOUNT_COMBINER)]
    reference = None
    f, writer = open_csv("combiner.csv", [
        "case", "trial", "seconds", "intermediate_bytes", "map_seconds", "reduce_seconds", "identical",
    ])
    with f:
        for trial in range(1, trials + 1):
            for name, job_file in rotated(cases, trial):
                set_live_workers(stub, ALL_WORKERS)
                result = run_job(stub, f"{name}-{trial}", job_file=job_file, reduce_order="largest_first")
                reference = reference or result["sha256"]
                writer.writerow([
                    name, trial, result["seconds"], result["intermediate_bytes"], result["map_seconds"],
                    result["reduce_seconds"], "yes" if result["sha256"] == reference else "NO",
                ])
                f.flush()


def reduce_order_experiment(stub, trials: int) -> None:
    print("reduce_order: numerical against largest first", flush=True)
    cases = ["numerical", "largest_first"]
    f, writer = open_csv("reduce_order.csv", [
        "case", "trial", "seconds", "map_seconds", "reduce_seconds", "largest_start",
    ])
    with f:
        for trial in range(1, trials + 1):
            for order in rotated(cases, trial):
                set_live_workers(stub, ALL_WORKERS)
                result = run_job(stub, f"order-{order}-{trial}", reduce_order=order)
                writer.writerow([
                    order, trial, result["seconds"], result["map_seconds"], result["reduce_seconds"],
                    result["largest_start"],
                ])
                f.flush()


def set_cpu_limit(worker: int, cpus: float) -> None:
    docker("update", "--cpus", str(cpus), f"mr-worker-{worker}")


def backup_experiment(stub, trials: int) -> None:
    print(f"backup: worker-{SLOW} limited to {SLOW_CPUS} CPU", flush=True)
    cases = [
        # name, is the slow worker slowed down, are backup tasks on
        ("healthy", False, True),
        ("slow_worker_no_backups", True, False),
        ("slow_worker_backups", True, True),
    ]
    reference = None
    f, writer = open_csv("backup.csv", ["case", "trial", "seconds", "backups", "identical"])
    try:
        with f:
            for trial in range(1, trials + 1):
                for name, slowed, backups in rotated(cases, trial):
                    set_live_workers(stub, ALL_WORKERS)
                    set_cpu_limit(SLOW, SLOW_CPUS if slowed else 1)
                    result = run_job(
                        stub, f"backup-{name}-{trial}", reduce_order="largest_first", backup_tasks=backups
                    )
                    reference = reference or result["sha256"]
                    writer.writerow([
                        name, trial, result["seconds"], result["backups"],
                        "yes" if result["sha256"] == reference else "NO",
                    ])
                    f.flush()

        # One more run with the slow worker and backups on, kept for drawing.
        # A run in which a backup was actually started is the one worth drawing.
        set_live_workers(stub, ALL_WORKERS)
        set_cpu_limit(SLOW, SLOW_CPUS)
        for attempt in range(1, 4):
            result = run_job(stub, "backup-timeline", reduce_order="largest_first", backup_tasks=True)
            if result["backups"]:
                break
        else:
            print("  warning: no backup was started in 3 tries; the timeline will show none", flush=True)
        shutil.copyfile(RESULTS / "events" / "backup-timeline.jsonl", RESULTS / "backup_timeline_events.jsonl")
        meta = {
            "slow_worker": f"worker-{SLOW}",
            "cpus": SLOW_CPUS,
            "seconds": result["seconds"],
            "backups": result["backups"],
            "maps": MAPS,
            "reduces": REDUCES,
        }
        (RESULTS / "backup_timeline_meta.json").write_text(json.dumps(meta, indent=2) + "\n")
    finally:
        set_cpu_limit(SLOW, 1)


def prepare_data(size_mb: float) -> None:
    """Generate the dataset if needed and put it in shared storage."""
    marker = DATA / ".size-mb"
    if not marker.exists() or marker.read_text().strip() != str(size_mb):
        shutil.rmtree(DATA, ignore_errors=True)
        subprocess.run(
            [sys.executable, str(REPO / "scripts" / "gen_data.py"),
             "--out", str(DATA), "--size-mb", str(size_mb), "--files", "8", "--seed", "0"],
            check=True,
        )
        marker.write_text(str(size_mb))
    shutil.rmtree(SHARED / INPUT, ignore_errors=True)
    upload(DATA, SHARED, INPUT)


def boss_heartbeat_timeout() -> float:
    """The heartbeat timeout the running boss was started with."""
    env = docker("inspect", "--format", "{{range .Config.Env}}{{println .}}{{end}}", "mr-boss")
    for line in env.splitlines():
        if line.startswith("MR_HEARTBEAT_TIMEOUT="):
            return float(line.split("=", 1)[1])
    return 5.0  # the boss's default


def worker_cpu_limit() -> float:
    """The CPU limit of a worker container, in CPUs."""
    return int(docker("inspect", "--format", "{{.HostConfig.NanoCpus}}", "mr-worker-1")) / 1e9


def write_environment(args, experiments: list) -> None:
    """Record the machine and the settings used. The experiments can be run
    one at a time with different settings, so those are kept per experiment."""
    path = RESULTS / "environment.json"
    info = json.loads(path.read_text()) if path.exists() else {}
    settings = info.setdefault("experiments", {})
    for name in experiments:
        settings[name] = {
            "trials": 1 if name == "timeline" else args.trials,
            "cooldown_seconds": args.cooldown,
            "heartbeat_timeout_seconds": boss_heartbeat_timeout(),
            "worker_cpu_limit": worker_cpu_limit(),
        }
    info.update({
        "date": time.strftime("%Y-%m-%d"),
        "machine": platform.platform(),
        "docker_cpus": int(docker("info", "--format", "{{.NCPU}}")),
        "docker_memory_gb": round(int(docker("info", "--format", "{{.MemTotal}}")) / 2**30, 1),
        "docker_version": docker("version", "--format", "{{.Server.Version}}"),
        "dataset_mb": args.size_mb,
        "map_tasks": MAPS,
        "reduce_tasks": REDUCES,
    })
    # These two used to be recorded once for all experiments.
    info.pop("heartbeat_timeout_seconds", None)
    info.pop("worker_cpu_limit", None)
    path.write_text(json.dumps(info, indent=2) + "\n")


EXPERIMENTS = {
    "scaling": scaling_experiment,
    "failure": failure_experiment,
    "timeline": lambda stub, trials: timeline_experiment(stub),
    "combiner": combiner_experiment,
    "reduce_order": reduce_order_experiment,
    "backup": backup_experiment,
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the performance experiments.")
    parser.add_argument("--size-mb", type=float, default=100.0, help="dataset size in megabytes (default: 100)")
    parser.add_argument("--trials", type=int, default=3, help="runs per configuration (default: 3)")
    parser.add_argument("--only", choices=list(EXPERIMENTS), help="run just one experiment")
    parser.add_argument("--cooldown", type=float, default=0,
                        help="seconds to pause before each run, so that a machine that slows down "
                             "when it gets hot (a laptop) can recover (default: 0)")
    args = parser.parse_args()
    global COOLDOWN
    COOLDOWN = args.cooldown

    (RESULTS / "events").mkdir(parents=True, exist_ok=True)
    docker("compose", "up", "-d", "--build")
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = pb_grpc.BossStub(channel)
        wait_for(lambda: boss_is_up(stub), "the boss is reachable")
        prepare_data(args.size_mb)
        chosen = [args.only] if args.only else list(EXPERIMENTS)
        write_environment(args, chosen)
        started = time.monotonic()
        for name in chosen:
            EXPERIMENTS[name](stub, args.trials)
    print(f"done in {(time.monotonic() - started) / 60:.1f} minutes; results are in {RESULTS}")


if __name__ == "__main__":
    main()
