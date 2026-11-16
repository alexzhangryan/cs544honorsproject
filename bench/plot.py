"""Draw the evaluation plots from the results of run_benchmarks.py.

    python bench/plot.py [--results bench/results] [--out bench/plots]

Writes one PNG file per result that is present:

  scaling.png          job time against the number of live workers
  failure.png          job time with and without a worker being killed
  timeline.png         which worker ran which task in one run with a failure
  combiner.png         intermediate data and job time, with and without a combiner
  reduce_order.png     the reduce phase with tasks in numerical order and largest first
  backup.png           job time with one slow worker, with and without backup tasks
  backup_timeline.png  which worker ran which task in one run with a backup task
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from statistics import median
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
from matplotlib.patches import Patch  # noqa: E402

REPO = Path(__file__).resolve().parent.parent

# Colours. Text is always drawn in a text colour, never in a data colour.
SURFACE = "#fcfcfb"
TEXT = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
GRID = "#e6e5e0"
BLUE = "#2a78d6"
LIGHT_BLUE = "#cde2fb"
ORANGE = "#eb6834"
LIGHT_ORANGE = "#fad7c8"
GRAY = "#b5b4ae"
RED = "#d03b3b"

plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "font.size": 10,
    "text.color": TEXT,
    "axes.labelcolor": TEXT_SECONDARY,
    "axes.edgecolor": GRID,
    "xtick.color": TEXT_SECONDARY,
    "ytick.color": TEXT_SECONDARY,
    "figure.facecolor": SURFACE,
    "axes.facecolor": SURFACE,
    "savefig.facecolor": SURFACE,
    "hatch.linewidth": 1.2,
})


def new_figure(title: str, subtitle: list, height: float = 4.4, gap: float = 0.3, panels: int = 1):
    """A figure with a title, a subtitle of one or more lines, and one plot
    (or several side by side) starting `gap` inches below the subtitle.
    Sizes are in inches. Returns the figure and the plot, or the list of plots."""
    fig, axes = plt.subplots(1, panels, figsize=(8, height), squeeze=False)
    axes = list(axes[0])
    fig.text(0.02, 1 - 0.3 / height, title, fontsize=13, fontweight="bold", va="center")
    for number, line in enumerate(subtitle):
        fig.text(0.02, 1 - (0.58 + 0.2 * number) / height, line,
                 fontsize=9, color=TEXT_SECONDARY, va="center")
    fig.subplots_adjust(top=1 - (0.58 + 0.2 * (len(subtitle) - 1) + gap) / height)
    for ax in axes:
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        ax.tick_params(length=0)
        ax.set_axisbelow(True)
    return fig, (axes[0] if panels == 1 else axes)


def median_bars(ax, rows: list, unit: str, decimals: int = 1, labels: bool = True) -> None:
    """Horizontal bars of medians with one dot per run.

    rows is a list of (label, values, colour). The median is written at the
    right-hand edge, clear of the dots.
    """
    longest = max(max(values) for _, values, _ in rows)
    for position, (label, values, color) in enumerate(rows):
        values = sorted(values)
        ax.barh(position, median(values), height=0.42, color=color)
        ys = [position + offset for offset in spread(len(values), 0.22)]
        ax.scatter(values, ys, s=14, color=TEXT, alpha=0.8, linewidth=0, zorder=3)
        ax.text(longest * 1.4, position, f"{median(values):.{decimals}f} {unit}", va="center", ha="right",
                fontsize=9, bbox={"facecolor": SURFACE, "edgecolor": "none", "pad": 2})
    ax.set_yticks(range(len(rows)))
    ax.set_yticklabels([label for label, _, _ in rows] if labels else [], color=TEXT, fontsize=9)
    ax.invert_yaxis()
    ax.set_xlim(0, longest * 1.4)
    ax.grid(axis="x", color=GRID, linewidth=1)
    ax.spines["bottom"].set_visible(False)


def read_csv(path: Path) -> list:
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def spread(count: int, width: float) -> list:
    """Offsets that place `count` dots side by side around zero."""
    if count == 1:
        return [0.0]
    return [width * (index / (count - 1) - 0.5) for index in range(count)]


def describe_workload(results: Path, runs: int) -> str:
    env = json.loads((results / "environment.json").read_text())
    return (
        f"Word count over {env['dataset_mb']:g} MB, {env['map_tasks']} map and "
        f"{env['reduce_tasks']} reduce tasks, {runs} runs per configuration."
    )


def plot_scaling(results: Path, out: Path) -> None:
    times = defaultdict(list)
    for row in read_csv(results / "scaling.csv"):
        times[int(row["workers"])].append(float(row["seconds"]))
    counts = sorted(times)
    medians = [median(times[n]) for n in counts]
    ideal = [medians[0] * counts[0] / n for n in counts]

    fig, ax = new_figure(
        "More workers finish the job sooner",
        [describe_workload(results, len(times[counts[0]])),
         "Each small dot is one run; the line joins the medians."],
    )
    ax.plot(counts, ideal, color=GRAY, linewidth=1.5, linestyle=(0, (4, 3)))
    ax.plot(counts, medians, color=BLUE, linewidth=2, solid_capstyle="round", solid_joinstyle="round")
    ax.scatter(counts, medians, s=70, color=BLUE, edgecolor=SURFACE, linewidth=2, zorder=3)
    for n in counts:
        xs = [n + offset for offset in spread(len(times[n]), 0.16)]
        ax.scatter(xs, sorted(times[n]), s=14, color=TEXT_SECONDARY, alpha=0.75, linewidth=0, zorder=4)

    for n, m in zip(counts, medians):
        label = f"{m:.1f} s"
        if n != counts[0]:
            label += f"\n{medians[0] / m:.2f}× faster"
        ax.annotate(label, (n, max(times[n])), xytext=(11, 6), textcoords="offset points",
                    fontsize=9, va="bottom", ha="left", linespacing=1.3)

    ax.set_xticks(counts)
    ax.set_xlim(counts[0] - 0.3, counts[-1] + 0.75)
    ax.set_ylim(0, max(max(values) for values in times.values()) * 1.2)
    ax.set_xlabel("live workers (1 CPU each)")
    ax.set_ylabel("job time (seconds)")
    ax.grid(axis="y", color=GRID, linewidth=1)
    ax.spines["left"].set_visible(False)
    ax.legend(
        handles=[
            Line2D([], [], color=BLUE, linewidth=2, marker="o", markersize=7,
                   markeredgecolor=SURFACE, label="measured (median)"),
            Line2D([], [], color=GRAY, linewidth=1.5, linestyle=(0, (4, 3)),
                   label="ideal: one-worker time ÷ workers"),
        ],
        loc="upper right", frameon=False, fontsize=9,
    )
    fig.savefig(out / "scaling.png", dpi=200)
    plt.close(fig)


FAILURE_CASES = [
    # key in failure.csv, label, whether a worker is killed
    ("no_failure", "4 workers,\nno failure", False),
    ("kill_during_map", "4 workers, one killed\nduring the map phase", True),
    ("kill_during_reduce", "4 workers, one killed\nduring the reduce phase", True),
    ("three_workers", "3 workers\nfrom the start", False),
]


def plot_failure(results: Path, out: Path) -> None:
    rows = read_csv(results / "failure.csv")
    times = defaultdict(list)
    for row in rows:
        times[row["case"]].append(float(row["seconds"]))
    identical = sum(row["identical"] == "yes" for row in rows)
    baseline = median(times["no_failure"])

    fig, ax = new_figure(
        "Losing a worker costs a few seconds, not the job",
        [describe_workload(results, len(times["no_failure"])),
         f"The output was identical to the failure-free output in {identical} of {len(rows)} runs."],
        height=4.2,
        gap=0.55,
    )
    longest = max(max(values) for values in times.values())
    for position, (case, label, killed) in enumerate(FAILURE_CASES):
        values = sorted(times[case])
        m = median(values)
        ax.barh(position, m, height=0.42, color=BLUE if killed else GRAY)
        ys = [position + offset for offset in spread(len(values), 0.22)]
        ax.scatter(values, ys, s=14, color=TEXT, alpha=0.8, linewidth=0, zorder=3)
        text = f"{m:.1f} s"
        if case != "no_failure":
            text += f"   (+{m - baseline:.1f} s)"
        # The values sit in a column at the right edge, clear of the dots.
        ax.text(longest * 1.38, position, text, va="center", ha="right", fontsize=9,
                bbox={"facecolor": SURFACE, "edgecolor": "none", "pad": 2})

    ax.set_yticks(range(len(FAILURE_CASES)))
    ax.set_yticklabels([label for _, label, _ in FAILURE_CASES], color=TEXT, fontsize=9)
    ax.invert_yaxis()
    ax.set_xlim(0, longest * 1.38)
    ax.set_xlabel("job time (seconds); bars are medians, dots are individual runs")
    ax.grid(axis="x", color=GRID, linewidth=1)
    ax.spines["bottom"].set_visible(False)
    ax.legend(
        handles=[Patch(color=BLUE, label="a worker is killed mid-job"), Patch(color=GRAY, label="no failure")],
        loc="lower left", bbox_to_anchor=(-0.01, 1.0), ncol=2, frameon=False, fontsize=9,
    )
    fig.subplots_adjust(left=0.24, bottom=0.15)
    fig.savefig(out / "failure.png", dpi=200)
    plt.close(fig)


def load_attempts(events: list) -> list:
    """Turn the boss's event log into one record per task attempt."""
    attempts = {}
    for event in events:
        key = (event.get("kind"), event.get("task"), event.get("attempt"))
        if event["event"] == "assigned":
            attempts[key] = {
                "kind": event["kind"], "task": event["task"], "attempt": event["attempt"],
                "worker": event["worker"], "start": event["t"], "end": None, "lost_at": None,
            }
        elif event["event"] == "completed":
            attempts[key]["end"] = event["t"]
        elif event["event"] == "requeued":
            attempts[key]["lost_at"] = event["t"]
    return list(attempts.values())


def plot_timeline(results: Path, out: Path) -> None:
    lines = (results / "timeline_events.jsonl").read_text().splitlines()
    events = [json.loads(line) for line in lines]
    meta = json.loads((results / "timeline_meta.json").read_text())
    attempts = load_attempts(events)
    workers = sorted({attempt["worker"] for attempt in attempts})
    row = {worker: index for index, worker in enumerate(workers)}
    kill_t = meta["kill_t"]
    job_end = events[-1]["t"]
    lost = next(attempt for attempt in attempts if attempt["lost_at"] is not None)
    rerun = next(
        attempt for attempt in attempts
        if (attempt["kind"], attempt["task"]) == (lost["kind"], lost["task"]) and attempt["end"] is not None
    )
    detection = lost["lost_at"] - kill_t

    fig, ax = new_figure(
        "A killed worker's task is re-run on another worker",
        [f"One run of word count ({meta['maps']} map and {meta['reduces']} reduce tasks). "
         f"Each bar is one task on the worker that ran it."],
        height=4.3,
    )
    height = 0.5
    colors = {"map": BLUE, "reduce": ORANGE}
    for attempt in attempts:
        y = row[attempt["worker"]]
        if attempt["end"] is None:  # cut short when its worker was killed
            ax.barh(y, kill_t - attempt["start"], left=attempt["start"], height=height,
                    facecolor=LIGHT_BLUE, edgecolor=BLUE, hatch="////", linewidth=0)
            continue
        ax.barh(y, attempt["end"] - attempt["start"], left=attempt["start"], height=height,
                color=colors[attempt["kind"]], edgecolor=SURFACE, linewidth=1.5)
    # The re-run of the lost task, drawn again on top with a dark outline.
    y = row[rerun["worker"]]
    ax.barh(y, rerun["end"] - rerun["start"], left=rerun["start"], height=height,
            facecolor=colors[rerun["kind"]], edgecolor=TEXT, linewidth=1.8, zorder=3)

    name = f"{lost['kind']} task {lost['task']}"
    victim_y = row[meta["victim"]]
    top, bottom = -0.5, len(workers) - 0.5
    ax.plot([kill_t, kill_t], [top, bottom], color=RED, linewidth=1.5, zorder=2)
    ax.text(kill_t, top - 0.08, f"{meta['victim']} is killed at {kill_t:.1f} s",
            ha="center", va="bottom", fontsize=9)
    ax.text(job_end, top - 0.08, f"job done at {job_end:.1f} s", ha="right", va="bottom",
            fontsize=9, color=TEXT_SECONDARY)
    # How long the boss took to notice.
    ax.annotate("", xy=(lost["lost_at"], victim_y), xytext=(kill_t, victim_y),
                arrowprops={"arrowstyle": "<->", "color": TEXT_SECONDARY, "linewidth": 1.2,
                            "shrinkA": 2, "shrinkB": 0})
    ax.text((kill_t + lost["lost_at"]) / 2, victim_y - 0.14,
            f"presumed dead\n{detection:.1f} s after the kill", ha="center", va="bottom",
            fontsize=8.5, color=TEXT_SECONDARY, linespacing=1.25)
    ax.text(kill_t - job_end * 0.008, victim_y + height / 2 + 0.06, f"{name}: lost",
            ha="right", va="top", fontsize=8.5)
    ax.text(rerun["start"], y + height / 2 + 0.06, f"{name}: run again", ha="left", va="top", fontsize=8.5)

    ax.set_yticks(range(len(workers)))
    ax.set_yticklabels(workers, color=TEXT)
    ax.set_ylim(len(workers) - 0.4, -0.95)
    ax.set_xlim(0, job_end * 1.01)
    ax.set_xlabel("seconds since the job was submitted")
    ax.grid(axis="x", color=GRID, linewidth=1)
    for side in ("left", "bottom"):
        ax.spines[side].set_visible(False)
    ax.legend(
        handles=[
            Patch(color=BLUE, label="map task"),
            Patch(color=ORANGE, label="reduce task"),
            Patch(facecolor=LIGHT_BLUE, edgecolor=BLUE, hatch="////", linewidth=0, label="lost when its worker was killed"),
            Patch(facecolor=BLUE, edgecolor=TEXT, linewidth=1.5, label="the lost task, run again"),
        ],
        loc="upper center", bbox_to_anchor=(0.5, -0.19), ncol=4, frameon=False, fontsize=8.5,
        handlelength=1.6, columnspacing=1.4,
    )
    fig.subplots_adjust(left=0.1, right=0.97, bottom=0.24)
    fig.savefig(out / "timeline.png", dpi=200)
    plt.close(fig)


def column(rows: list, case: str, name: str) -> list:
    return [float(row[name]) for row in rows if row["case"] == case]


def plot_combiner(results: Path, out: Path) -> None:
    rows = read_csv(results / "combiner.csv")
    megabytes = {
        case: [value / 2**20 for value in column(rows, case, "intermediate_bytes")]
        for case in ("without_combiner", "with_combiner")
    }
    seconds = {case: column(rows, case, "seconds") for case in megabytes}
    shrink = median(megabytes["without_combiner"]) / median(megabytes["with_combiner"])
    identical = sum(row["identical"] == "yes" for row in rows)

    fig, (left, right) = new_figure(
        f"A combiner cuts the intermediate data by a factor of {shrink:.0f}",
        [describe_workload(results, len(seconds["with_combiner"])),
         f"Bars are medians, dots are individual runs. Same output in {identical} of {len(rows)} runs."],
        height=3.3, gap=0.55, panels=2,
    )
    names = {"without_combiner": "without\ncombine_fn", "with_combiner": "with\ncombine_fn"}
    colors = {"without_combiner": GRAY, "with_combiner": BLUE}
    median_bars(left, [(names[c], megabytes[c], colors[c]) for c in names], "MB")
    median_bars(right, [(names[c], seconds[c], colors[c]) for c in names], "s", labels=False)
    left.set_title("intermediate data (MB)", loc="left", fontsize=9.5, color=TEXT_SECONDARY)
    right.set_title("job time (seconds)", loc="left", fontsize=9.5, color=TEXT_SECONDARY)
    fig.subplots_adjust(left=0.13, right=0.97, bottom=0.12, wspace=0.12)
    fig.savefig(out / "combiner.png", dpi=200)
    plt.close(fig)


def plot_reduce_order(results: Path, out: Path) -> None:
    rows = read_csv(results / "reduce_order.csv")
    cases = {"numerical": "numerical\norder", "largest_first": "largest\nfirst"}
    colors = {"numerical": GRAY, "largest_first": BLUE}
    phase = {case: column(rows, case, "reduce_seconds") for case in cases}
    start = {case: column(rows, case, "largest_start") for case in cases}
    saved = median(phase["numerical"]) - median(phase["largest_first"])
    if saved >= 0.3:
        title = f"Starting the largest reduce task first shortens the reduce phase by {saved:.1f} s"
    else:
        title = "Starting the largest reduce task first makes little difference here"

    fig, (left, right) = new_figure(
        title,
        [describe_workload(results, len(phase["numerical"])),
         "Bars are medians, dots are individual runs. Times come from the boss's scheduling log."],
        height=3.3, gap=0.55, panels=2,
    )
    median_bars(left, [(cases[c], phase[c], colors[c]) for c in cases], "s")
    median_bars(right, [(cases[c], start[c], colors[c]) for c in cases], "s", labels=False)
    left.set_title("length of the reduce phase (seconds)", loc="left", fontsize=9.5, color=TEXT_SECONDARY)
    right.set_title("largest reduce task starts (seconds in)", loc="left",
                    fontsize=9.5, color=TEXT_SECONDARY)
    fig.subplots_adjust(left=0.13, right=0.97, bottom=0.12, wspace=0.12)
    fig.savefig(out / "reduce_order.png", dpi=200)
    plt.close(fig)


def plot_backup(results: Path, out: Path) -> None:
    rows = read_csv(results / "backup.csv")
    meta = json.loads((results / "backup_timeline_meta.json").read_text())
    slow = f"{meta['slow_worker']} at {meta['cpus']:g} CPU"
    cases = [
        ("healthy", "4 healthy workers", GRAY),
        ("slow_worker_no_backups", f"{slow},\nno backup tasks", GRAY),
        ("slow_worker_backups", f"{slow},\nbackup tasks on", BLUE),
    ]
    seconds = {case: column(rows, case, "seconds") for case, _, _ in cases}
    saved = median(seconds["slow_worker_no_backups"]) - median(seconds["slow_worker_backups"])
    identical = sum(row["identical"] == "yes" for row in rows)

    fig, ax = new_figure(
        f"Backup tasks win back {saved:.1f} s that a slow worker would cost",
        [describe_workload(results, len(seconds["healthy"])),
         f"Bars are medians, dots are individual runs. Same output in {identical} of {len(rows)} runs."],
        height=3.6, gap=0.35,
    )
    median_bars(ax, [(label, seconds[case], color) for case, label, color in cases], "s")
    ax.set_xlabel("job time (seconds)")
    fig.subplots_adjust(left=0.26, right=0.97, bottom=0.17)
    fig.savefig(out / "backup.png", dpi=200)
    plt.close(fig)


def plot_backup_timeline(results: Path, out: Path) -> None:
    events = [json.loads(line) for line in (results / "backup_timeline_events.jsonl").read_text().splitlines()]
    meta = json.loads((results / "backup_timeline_meta.json").read_text())
    attempts = {}
    for event in events:
        key = (event.get("kind"), event.get("task"), event.get("attempt"))
        if event["event"] == "assigned":
            attempts[key] = {**event, "start": event["t"], "end": None, "stopped": False}
        elif event["event"] == "completed":
            attempts[key]["end"] = event["t"]
        elif event["event"] in ("cancelled", "attempt_failed", "lost", "requeued"):
            attempts[key].update(end=event["t"], stopped=True)
    workers = sorted({attempt["worker"] for attempt in attempts.values()})
    row = {worker: index for index, worker in enumerate(workers)}
    job_end = events[-1]["t"]

    title = "A free worker takes a second copy of the slow worker's task"
    if not meta.get("backups"):
        title = "No backup copy was started in this run"
    fig, ax = new_figure(
        title,
        [f"One run of word count ({meta['maps']} map and {meta['reduces']} reduce tasks) with "
         f"{meta['slow_worker']} limited to {meta['cpus']:g} CPU.",
         "Each bar is one task on the worker that ran it."],
        height=4.5,
    )
    height = 0.5
    solid = {"map": BLUE, "reduce": ORANGE}
    light = {"map": LIGHT_BLUE, "reduce": LIGHT_ORANGE}
    for attempt in attempts.values():
        y = row[attempt["worker"]]
        end = attempt["end"] if attempt["end"] is not None else job_end
        if attempt["stopped"]:
            ax.barh(y, end - attempt["start"], left=attempt["start"], height=height,
                    facecolor=light[attempt["kind"]], edgecolor=solid[attempt["kind"]], hatch="////", linewidth=0)
        else:
            ax.barh(y, end - attempt["start"], left=attempt["start"], height=height,
                    color=solid[attempt["kind"]], edgecolor=SURFACE, linewidth=1.5)
    for attempt in attempts.values():
        if attempt.get("backup"):
            y = row[attempt["worker"]]
            end = attempt["end"] if attempt["end"] is not None else job_end
            ax.barh(y, end - attempt["start"], left=attempt["start"], height=height,
                    facecolor="none", edgecolor=TEXT, linewidth=1.8, zorder=3)
            # Anchor the label on the side of the bar that has room for it.
            on_right = attempt["start"] > job_end / 2
            ax.text(end if on_right else attempt["start"], y + height / 2 + 0.06,
                    f"backup copy of {attempt['kind']} task {attempt['task']}",
                    ha="right" if on_right else "left", va="top", fontsize=8.5)

    ax.text(job_end, -0.58, f"job done at {job_end:.1f} s", ha="right", va="bottom",
            fontsize=9, color=TEXT_SECONDARY)
    ax.set_yticks(range(len(workers)))
    ax.set_yticklabels(
        [f"{worker}\n({meta['cpus']:g} CPU)" if worker == meta["slow_worker"] else worker for worker in workers],
        color=TEXT,
    )
    ax.set_ylim(len(workers) - 0.4, -0.95)
    ax.set_xlim(0, job_end * 1.01)
    ax.set_xlabel("seconds since the job was submitted")
    ax.grid(axis="x", color=GRID, linewidth=1)
    for side in ("left", "bottom"):
        ax.spines[side].set_visible(False)
    ax.legend(
        handles=[
            Patch(color=BLUE, label="map task"),
            Patch(color=ORANGE, label="reduce task"),
            Patch(facecolor="none", edgecolor=TEXT, linewidth=1.5, label="backup copy"),
            Patch(facecolor=LIGHT_ORANGE, edgecolor=ORANGE, hatch="////", linewidth=0,
                  label="stopped: the other copy finished first"),
        ],
        loc="upper center", bbox_to_anchor=(0.5, -0.19), ncol=4, frameon=False, fontsize=8.5,
        handlelength=1.6, columnspacing=1.4,
    )
    fig.subplots_adjust(left=0.11, right=0.97, bottom=0.24)
    fig.savefig(out / "backup_timeline.png", dpi=200)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(description="Draw the evaluation plots.")
    parser.add_argument("--results", default=str(REPO / "bench" / "results"))
    parser.add_argument("--out", default=str(REPO / "bench" / "plots"))
    args = parser.parse_args()
    results, out = Path(args.results), Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    plots = [
        # result file that must be there, function that draws from it
        ("scaling.csv", plot_scaling),
        ("failure.csv", plot_failure),
        ("timeline_events.jsonl", plot_timeline),
        ("combiner.csv", plot_combiner),
        ("reduce_order.csv", plot_reduce_order),
        ("backup.csv", plot_backup),
        ("backup_timeline_events.jsonl", plot_backup_timeline),
    ]
    for needed, draw in plots:
        if (results / needed).exists():
            draw(results, out)
            print(f"drew {draw.__name__.removeprefix('plot_')}.png")


if __name__ == "__main__":
    main()
