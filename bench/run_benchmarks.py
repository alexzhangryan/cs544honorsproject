"""Run the performance experiments on the real Docker Compose cluster.

    python bench/run_benchmarks.py [--size-mb 100] [--trials 3] [--only NAME]

Three experiments, all running word count with 16 map and 8 reduce tasks:

  scaling   job time with 1, 2, 3 and 4 live workers
  failure   job time with no failure, with a worker killed during the map
            phase, with a worker killed during the reduce phase, and with
            only three workers from the start
  timeline  one run with a worker killed during the map phase, keeping the
            boss's scheduling log so the recovery can be drawn

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
INPUT = "input/bench"
MAPS = 16
REDUCES = 8
ALL_WORKERS = [1, 2, 3, 4]
VICTIM = 2  # the worker that gets killed in the failure experiments
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


def run_job(stub, name: str, kill: str = "") -> dict:
    """Run word count once and return its measurements. The boss's
    scheduling log for the run is kept in bench/results/events/."""
    output = f"output/bench-{name}"
    shutil.rmtree(SHARED / output, ignore_errors=True)
    time.sleep(COOLDOWN)
    job_id = submit_job(stub, INPUT, output, WORDCOUNT, MAPS, REDUCES)
    kill_t = kill_victim(stub, job_id, kill) if kill else None
    status = wait_for_job(stub, job_id, poll=0.1)
    if status.state != "SUCCEEDED":
        raise SystemExit(f"job {job_id} failed:\n{status.error}")

    result = {
        "job_id": job_id,
        "seconds": round(status.elapsed_seconds, 2),
        "requeues": status.requeues,
        "sha256": output_hash(SHARED / output),
        "kill_t": kill_t,
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
            # Start each trial with a different case. A machine that warms up
            # and slows down within a trial then affects every case equally;
            # with four trials each case runs once in each position.
            shift = (trial - 1) % len(cases)
            for name, live, kill in cases[shift:] + cases[:shift]:
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the performance experiments.")
    parser.add_argument("--size-mb", type=float, default=100.0, help="dataset size in megabytes (default: 100)")
    parser.add_argument("--trials", type=int, default=3, help="runs per configuration (default: 3)")
    parser.add_argument("--only", choices=["scaling", "failure", "timeline"], help="run just one experiment")
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
        write_environment(args, [args.only] if args.only else ["scaling", "failure", "timeline"])
        started = time.monotonic()
        if args.only in (None, "scaling"):
            scaling_experiment(stub, args.trials)
        if args.only in (None, "failure"):
            failure_experiment(stub, args.trials)
        if args.only in (None, "timeline"):
            timeline_experiment(stub)
    print(f"done in {(time.monotonic() - started) / 60:.1f} minutes; results are in {RESULTS}")


if __name__ == "__main__":
    main()
