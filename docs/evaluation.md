# Performance evaluation

Six experiments, all on the real Docker Compose cluster, all running word
count ([examples/wordcount.py](../examples/wordcount.py)) over 100 MB of text
with 16 map tasks and 8 reduce tasks. Sections 1 to 3 measure the base system
and its main feature, worker failure handling. Section 4 measures the three
features added afterwards.

**Results in brief**

- Four workers finish the job 2.75 times faster than one worker (16.4 s
  against 45.1 s).
- Killing a worker in the middle of a job delays the job by one to two
  seconds. The job always finishes.
- In all 56 timed runs of the failure experiment, 28 of them with a worker
  killed mid-job, the output was byte-identical to the failure-free output.
- A combiner cuts word count's intermediate data from 263 MB to 1.2 MB and
  the job from 15.4 s to 2.2 s.
- Handing out the largest reduce task first shortens the reduce phase from
  7.3 s to 5.6 s.
- With one worker slowed to a quarter of a CPU, backup tasks bring the job
  from 26.5 s down to 21.6 s.

## Setup

| | |
|---|---|
| Machine | MacBook Air, Apple M2 (4 performance and 4 efficiency cores), 16 GB RAM |
| Docker | Docker Desktop, engine 29.3.0. Its Linux VM has 8 CPUs and 7.7 GB of memory |
| Cluster | As in [docker-compose.yml](../docker-compose.yml): one boss and four workers, each worker limited to 1 CPU; heartbeat timeout 5 s |
| Data | 100 MB of text in 8 files from [scripts/gen_data.py](../scripts/gen_data.py), seed 0. Word frequencies follow a Zipf distribution, as in real text |
| Job | Word count, 16 map tasks, 8 reduce tasks |
| Timing | Measured by the boss, from the submission of the job to the completion of its last reduce task |

How the runs were made:

- Every configuration was run several times. The plots show every run as a
  dot, and the numbers quoted are medians.
- The configurations were interleaved: one run of each, then the next round.
  In the failure experiment the order also rotates from round to round, so
  each case runs once in each position.
- The script pauses before every run (25 s in the first experiment, 45 s in
  the second). This laptop has no fan and slows down when it gets hot.

Two things to keep in mind when reading the numbers:

- **This is not the course VM.** Absolute times will differ on a 4-CPU Linux
  VM. The commands to repeat the measurements are at the end.
- **The laptop was in use.** Other programs were running, and one system
  process used a full CPU core throughout. A few runs were visibly slowed by
  this. They are shown, not removed.

## 1. More workers finish the job sooner

The same job with one, two, three and four live workers. The other worker
containers were stopped.

![Job time against the number of live workers](../bench/plots/scaling.png)

| Live workers | Job time in each of 5 runs (s) | Median (s) | Speedup |
|---|---|---|---|
| 1 | 44.7, 44.9, 45.1, 47.4, 48.4 | 45.1 | 1.00× |
| 2 | 25.9, 25.9, 26.2, 26.7, 26.8 | 26.2 | 1.72× |
| 3 | 20.1, 20.1, 20.4, 20.4, 20.8 | 20.4 | 2.21× |
| 4 | 16.2, 16.3, 16.4, 16.4, 17.4 | 16.4 | 2.75× |

Tasks really are spread across the workers: every extra worker shortens the
job. The runs are repeatable, each within 8% of the median for its
configuration.

**Why 2.75× and not 4×?** The boss's scheduling log for each run
([bench/results/events/](../bench/results/events/)) gives two reasons.

| Live workers | Typical map task (s) | Map phase (s) | Reduce phase (s) |
|---|---|---|---|
| 1 | 1.77 | 28.6 | 16.5 |
| 2 | 1.96 | 16.2 | 10.0 |
| 3 | 2.07 | 12.4 | 8.0 |
| 4 | 2.28 | 9.3 | 7.0 |

1. **Tasks get slower when more of them run at once.** A map task takes
   1.77 s when one worker runs alone and 2.28 s when four run together, 29%
   longer. The workers share the machine's memory, the file sharing layer
   between macOS and the Docker VM, and four fast cores, one of which was
   busy with something else. The map phase on its own speeds up 3.1×.
2. **The reduce work is uneven.** Each word goes to one reduce task, and the
   task that gets the most common word has 27% of all the reduce work. It
   takes about 5 s, and the reduce phase cannot be shorter than that. The
   boss also hands out reduce tasks in numerical order, so this one (task 6)
   starts in the second round and finishes last. The reduce phase speeds up
   only 2.4×.

Handing out the largest reduce task first would shorten the four-worker
reduce phase by up to about 1.8 s. The boss could tell which is largest from
the sizes of the intermediate files. This is not implemented.

## 2. A worker failure costs a few seconds

Four cases:

- four workers and no failure;
- four workers, with `worker-2` killed during the map phase;
- four workers, with `worker-2` killed during the reduce phase;
- three workers from the start, for comparison.

The kill is `docker kill mr-worker-2`, one second after `worker-2` has
started a task, so it lands in the middle of the task's work. In the map case
this is after at least 4 of the 16 map tasks have completed. In the reduce
case it is early in the reduce phase, as soon as `worker-2` has spent a
second on a reduce task.

![Job time with and without a worker being killed](../bench/plots/failure.png)

| Case | Job time in each of 4 runs (s) | Median (s) | Extra over no failure | Tasks re-run | Output identical |
|---|---|---|---|---|---|
| 4 workers, no failure | 15.8, 17.1, 17.1, 18.8 | 17.1 | | 0 | 4 of 4 |
| 4 workers, one killed during map | 18.8, 18.9, 19.3, 29.9 | 19.1 | +2.1 s | 1 in every run | 4 of 4 |
| 4 workers, one killed during reduce | 17.2, 18.1, 18.2, 19.0 | 18.2 | +1.1 s | 1 in every run | 4 of 4 |
| 3 workers from the start | 20.6, 21.1, 21.7, 27.4 | 21.4 | +4.3 s | 0 | 4 of 4 |

**The job always finished, and its output was never wrong.** "Output
identical" compares the SHA-256 hash of the output files with that of the
first failure-free run.

**What the logs show for the eight runs with a kill:**

- The boss put the lost task back in the queue 6.1 to 6.5 s after handing it
  out. The worker was killed about one second into the task, so that is
  about 5 s after the kill, which is the configured heartbeat timeout.
- Exactly one task was re-run each time. The tasks `worker-2` had already
  completed were not re-run, because their output was in shared storage.
- The re-run took 2.1 to 3.4 s for a map task and 1.1 to 1.8 s for a reduce
  task.

**What this means:**

- A failure costs less than the 5 s it takes to detect. While the boss waits
  for the timeout, the other three workers keep working on other tasks. The
  delay only holds back the one lost task.
- A failure also costs less than starting with three workers (+4.3 s). The
  design document expected the two to be about equal, plus the detection
  delay. They are not: the job had four workers until the kill, and three
  workers each run a little faster than four (reason 1 in the previous
  section).
- The cost depends on which task is lost and when. In these runs the lost
  reduce task was always one of the small ones. Losing the largest reduce
  task late in the job would cost more: the job would wait for the timeout
  and then for that task to run again. That case was not measured.

**Two slow runs.** The 29.9 s and 27.4 s runs were both in the third round.
In those two runs every task on every worker was slower than usual, which
points to other activity on the laptop, not to the cluster. The medians do
not depend on them.

**Earlier passes of this experiment.** This experiment was run three times.
The first two passes, five rounds each, were discarded for timing: the
laptop was being used and was heating up, and the time of the failure-free
case ranged from 17 to 38 s. That is why the third pass has a longer pause
and a rotating order. The outputs of those 40 runs were checked in the same
way and were all identical too, which gives 56 runs in total, 28 of them
with a kill.

## 3. What a recovery looks like

One run with `worker-2` killed during the map phase, drawn from the boss's
scheduling log
([bench/results/timeline_events.jsonl](../bench/results/timeline_events.jsonl)).

![Which worker ran which task, with worker-2 killed at 3.6 s](../bench/plots/timeline.png)

| Time | What happened |
|---|---|
| 2.25 s | The boss hands map task 6 to `worker-2` |
| 3.58 s | `worker-2` is killed |
| 8.46 s | The boss has heard nothing from `worker-2` for 5 s, presumes it dead, and puts map task 6 back in the queue (4.9 s after the kill) |
| 8.55 s | `worker-1` asks for its next task and gets map task 6 |
| 10.62 s | `worker-1` finishes map task 6 |
| 10.93 s | The last map task finishes and the reduce phase starts, on three workers |
| 18.94 s | The job is done |

The picture shows the feature working as designed. The row of `worker-2`
stops at the kill. Its unfinished task reappears on another worker after the
timeout. The map task `worker-2` finished before the kill is not run again.
Between the kill and the timeout, the other three workers carry on with
other tasks.

The last task to finish is reduce task 6 on `worker-3`, which takes 5.0 s.
This is the oversized reduce task from section 1.

## 4. The additional features

Each experiment switches on only the feature it is about. The combiner, in
particular, removes nearly all of the intermediate data, which would leave
the other two features nothing to work on. Sections 1 to 3 were measured
before these features existed, so they describe the base system.

Each configuration was run 4 times, interleaved and in rotating order, with
a 25-second pause before each run.

### 4.1 Combiner

Word count with and without `combine_fn`
([examples/wordcount_combiner.py](../examples/wordcount_combiner.py)).

![Intermediate data and job time with and without a combiner](../bench/plots/combiner.png)

| | Intermediate data | Job time in each of 4 runs (s) | Median (s) | Output identical |
|---|---|---|---|---|
| without `combine_fn` | 263.1 MB | 14.6, 15.0, 15.8, 17.2 | 15.4 | 4 of 4 |
| with `combine_fn` | 1.2 MB | 2.1, 2.2, 2.2, 2.3 | 2.2 | 4 of 4 |

The intermediate data shrinks 228 times. The text has only 339 distinct
words, so a map task's 1.5 million pairs collapse to a few hundred pairs per
batch of 100,000.

The job becomes 7 times faster, for two reasons. The map phase no longer
writes 263 MB to shared storage (9.5 s down to 1.9 s), and the reduce phase
has almost nothing to read and group (5.9 s down to 0.3 s). Writing and
reading the intermediate data was most of the work, not counting words.

### 4.2 Largest reduce task first

Reduce tasks handed out in numerical order (the old behaviour) and largest
first. Times are taken from the boss's scheduling log.

![The reduce phase with tasks in numerical order and largest first](../bench/plots/reduce_order.png)

| | Reduce phase in each of 4 runs (s) | Median (s) | Largest task starts, median | Job, median (s) |
|---|---|---|---|---|
| numerical order | 7.1, 7.1, 7.4, 7.6 | 7.3 | 2.0 s into the phase | 16.9 |
| largest first | 5.6, 5.6, 5.6, 5.9 | 5.6 | at once | 15.0 |

This fixes the problem found in section 1. Reduce task 6 holds 27% of the
reduce work. In numerical order it was handed out in the second round, two
seconds into the phase, and the job waited for it alone at the end. Handed
out first, it runs while the other workers share out the smaller tasks. The
reduce phase is 1.7 s shorter, close to the 1.8 s estimated in section 1.
The four runs of each case do not overlap.

### 4.3 Backup tasks for a slow worker

`worker-3` limited to a quarter of a CPU with `docker update --cpus 0.25`,
so that its tasks take about four times as long. Three cases: four healthy
workers; the slow worker with backup tasks off; the slow worker with backup
tasks on.

![Job time with one slow worker, with and without backup tasks](../bench/plots/backup.png)

| | Job time in each of 4 runs (s) | Median (s) | Backup copies started | Output identical |
|---|---|---|---|---|
| 4 healthy workers | 15.6, 15.6, 18.5, 19.6 | 17.0 | 0 | 4 of 4 |
| slow worker, no backup tasks | 26.1, 26.1, 26.9, 31.4 | 26.5 | 0 | 4 of 4 |
| slow worker, backup tasks on | 20.9, 21.5, 21.7, 22.6 | 21.6 | 1 or 2 per run | 4 of 4 |

The slow worker costs 9.5 s without backups. Backup tasks win back 5.0 s of
that, a little over half.

They cannot win back all of it, for two reasons that the timeline below
shows. First, a backup is only started when a worker has nothing else to
do, which is near the end of a phase. Second, a task only counts as slow
once it is clearly late (1.5 times its expected time and at least a second),
so some time is always lost before the backup starts.

![Which worker ran which task, in a run with a backup copy](../bench/plots/backup_timeline.png)

In this run `worker-3` takes reduce task 2 at the start of the reduce phase
(12.6 s). Six seconds later the other workers have run out of reduce tasks
and reduce task 2 is clearly late, so `worker-4` gets a backup copy. The
backup finishes at 21.5 s, the job is done, and `worker-3` is told to stop.
`worker-3` also held up the map phase. That is not something backups can
help with here, because the other workers still had map tasks to do while it
was busy.

## Limits of these measurements

- One laptop, in use, without a fan. The numbers show how the system
  behaves, not how fast it is on server hardware.
- One job (word count) and one input size (100 MB).
- One worker killed per run. Losing several workers is covered by the tests
  but was not timed.
- The detection delay is the heartbeat timeout, 5 s. A shorter timeout would
  detect failures sooner, at a greater risk of mistaking a busy machine for a
  dead one. Other values were not tried.
- The scaling and timeline runs used the code as it was just before a last
  round of error-handling fixes in the boss. The failure runs used the code
  after those fixes. The fixes do not change how tasks are scheduled or run
  in a job without errors.
- The measurements in section 4 were taken just before the review of those
  features. The fixes made after the review (a stop instruction is repeated
  until the worker acts on it, and `combine_fn = None` means no combiner) do
  not change which tasks are handed out or when.
- The slow worker in section 4.3 is slowed by a CPU limit. A worker slowed by
  a failing disk or a busy network would behave differently.

## Repeating the measurements

With the cluster built and the client installed as in the
[README](../README.md):

```bash
pip install -r requirements-dev.txt
python bench/run_benchmarks.py --only scaling --trials 5 --cooldown 25
python bench/run_benchmarks.py --only failure --trials 4 --cooldown 45
python bench/run_benchmarks.py --only timeline --cooldown 25
python bench/run_benchmarks.py --only combiner --trials 4 --cooldown 25
python bench/run_benchmarks.py --only reduce_order --trials 4 --cooldown 25
python bench/run_benchmarks.py --only backup --trials 4 --cooldown 25
python bench/plot.py
```

This takes about 65 minutes. The pauses (`--cooldown`) are for a machine
that slows down when hot. On a machine with proper cooling,
`python bench/run_benchmarks.py` runs all six experiments without pauses in
about 25 minutes.

The script starts, stops and kills the worker containers, and generates the
dataset in `data/bench` if it is not there.

| File | Contents |
|---|---|
| [bench/results/scaling.csv](../bench/results/scaling.csv) | one row per run of experiment 1 |
| [bench/results/failure.csv](../bench/results/failure.csv) | one row per run of experiment 2, with the output hash |
| [bench/results/events/](../bench/results/events/) | the boss's scheduling log for every run |
| [bench/results/timeline_events.jsonl](../bench/results/timeline_events.jsonl) | the log drawn in section 3 |
| [bench/results/combiner.csv](../bench/results/combiner.csv), [reduce_order.csv](../bench/results/reduce_order.csv), [backup.csv](../bench/results/backup.csv) | one row per run of each experiment in section 4 |
| [bench/results/backup_timeline_events.jsonl](../bench/results/backup_timeline_events.jsonl) | the log drawn in section 4.3 |
| [bench/results/environment.json](../bench/results/environment.json) | the machine, and the settings of each experiment |
| [bench/plots/](../bench/plots/) | the seven plots |
