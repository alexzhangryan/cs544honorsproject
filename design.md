# MapReduce on Docker Compose: Design

CS 544 Honors Project 1 (solo project)

## 1. Summary

This is a small MapReduce system written in Python. One boss and four workers
run as containers under Docker Compose, and each worker is limited to one CPU.
A command-line client on the host uploads data, submits jobs and checks
progress.

The main special feature is **worker failure handling**. If a worker dies in
the middle of a job, the boss notices, re-runs the lost task on another worker,
and the job finishes with the same output a failure-free run would have
produced.

Three smaller features were added afterwards and are described in section 10:
**combiners**, handing out the **largest reduce task first**, and **backup
tasks for stragglers**.

## 2. Goals and non-goals

**Goals**

- Run user-supplied map and reduce functions over text files in shared
  storage, with a chosen number of map tasks and reduce tasks.
- Spread tasks over four 1-CPU workers, so a job finishes faster with more
  workers.
- Survive the loss of workers during a job, as long as at least one worker is
  alive or comes back.
- Be easy to try: Docker, Python and one `pip install`.
- Save work and waiting where that is cheap: combine map output before
  writing it, start the largest reduce task first, and duplicate a task that
  is stuck on a slow worker.

**Non-goals**

- Boss failure. The boss keeps job state in memory, so if it dies, running
  jobs are lost.
- More than one VM, or a distributed file system.
- Reduce partitions that do not fit in a worker's memory (reduce groups its
  input in memory).
- Data locality.
- Sandboxing user code, and authentication.

**Must never happen**

- A job that reports success with wrong output (missing, duplicated or partial
  records) after a failure.

## 3. Architecture

```
 host VM
 +--------------------------------------------------------------+
 |  client  (python -m mapreduce.client ...)                     |
 |    |  gRPC, localhost:50051            |  file copy           |
 |    v                                   v                      |
 |  +--------+     gRPC      +----------+      ./shared          |
 |  |  boss  | <------------ | worker-1 |      mounted as        |
 |  |        | <------------ | worker-2 |      /shared in        |
 |  |        | <------------ | worker-3 |      every container   |
 |  |        | <------------ | worker-4 |                        |
 |  +--------+               +----------+                        |
 |                           1 CPU each                          |
 +--------------------------------------------------------------+
```

The boss is the only server. Workers and the client always call the boss; the
boss never calls anyone.

**Life of a job**

1. The client sends the job to the boss: input path, output path, the source
   of the job file, the number of map tasks (M) and reduce tasks (R).
2. The boss saves the job file to shared storage and splits the input by bytes
   into M pieces of roughly equal size. A piece can span several files or be
   part of one file. A line belongs to the piece that contains its first byte,
   so every line is processed exactly once. There are always exactly M map
   tasks, even if some are empty.
3. Each idle worker asks the boss for a task. A map task reads its piece, calls
   `map_fn` on every line, and writes each output pair to one of R intermediate
   files, chosen by a hash of the key.
4. When every map task is complete, the boss hands out the R reduce tasks. A
   reduce task reads its M intermediate files, collects the values of each
   key, calls `reduce_fn` once per key in sorted key order, and writes one
   output file.
5. When every reduce task is complete, the boss marks the job succeeded and
   deletes the intermediate files.

A worker runs one task at a time. Several jobs can be submitted; the boss
serves them in submission order. An idle worker takes a task from the oldest
job that has one ready, so a later job can start while the last tasks of an
earlier job are still running.

## 4. Client interface

The client is a Python command-line program that runs on the host, outside any
container. It needs Python 3.10 or later and `pip install -r requirements.txt`
(gRPC only).

Paths for input and output are relative to the shared storage root.

**Upload starter data to shared storage**

```
python -m mapreduce.client upload data/sample input/sample
```

This copies the local file or directory `data/sample` to `input/sample` in
shared storage.

**Submit a job**

```
python -m mapreduce.client submit \
    --input input/sample \
    --output output/wordcount \
    --job examples/wordcount.py \
    --maps 8 \
    --reduces 4
```

`submit` prints the job id, then shows progress until the job finishes
(`--no-wait` returns immediately). The boss rejects a job whose output
directory already exists and is not empty, or is being written by a job that
is still running.

Two more options switch off a scheduling feature for one job:
`--no-backup-tasks` and `--reduce-order numerical` (section 10). They exist
so that the performance evaluation can compare a job with and without the
feature.

**Check on things**

```
python -m mapreduce.client status <job-id>     # progress, state, error if any
python -m mapreduce.client workers             # each worker and whether it is alive
```

**The job file** is an ordinary Python file that defines two generator
functions:

```python
def map_fn(key, value):
    # key: path of the input file, value: one line of text
    for word in value.split():
        yield word.lower(), 1

def reduce_fn(key, values):
    # key: a word, values: every value emitted for that key
    yield key, sum(values)
```

Rules for job files:

- Intermediate keys (the keys `map_fn` yields) must be strings.
- Values must be JSON-serialisable.
- Only the Python standard library can be imported.
- Both functions should be deterministic, because a task may run more than
  once after a failure.

A job file can also define a third function, `combine_fn`, described in
section 10.

## 5. Language

Everything is Python: boss, workers, client, tests and benchmark scripts. The
containers use Python 3.12; the client supports 3.10 or later.

## 6. Communication

All communication is gRPC with protobuf messages. The boss exposes one
service:

| Caller | RPC | Purpose |
|--------|-----|---------|
| client | `SubmitJob` | send paths, job file source, M and R; returns a job id |
| client | `GetJobStatus` | state, tasks done out of total, error text, elapsed time, size of the intermediate data |
| client | `ListWorkers` | each worker's id and whether it is alive |
| worker | `GetTask` | ask for the next task; the reply is a map task, a reduce task, or "nothing right now" |
| worker | `Heartbeat` | "I am still alive", sent while a task is running; the reply can tell the worker to stop the task |
| worker | `ReportTask` | task finished, or failed with an error message |

Workers pull work instead of the boss pushing it. This keeps one server in the
system, and a restarted worker rejoins simply by asking for a task.

Bulk data never travels over gRPC. Workers read and write it directly in
shared storage.

## 7. Shared storage

Shared storage is one host directory, `./shared`, bind-mounted at `/shared` in
the boss and in every worker. The project is demonstrated on a single VM, so
this gives every container the same view of the data with no extra services,
and the client can upload by copying files.

```
shared/
  input/<dataset>/...                      uploaded by the client
  output/<name>/part-00000.txt ...         one file per reduce task
  jobs/<job-id>/job.py                     the user's map and reduce functions
  jobs/<job-id>/events.jsonl               scheduling log (used for the plots)
  jobs/<job-id>/intermediate/              deleted when the job succeeds
      map-<m>-reduce-<r>.jsonl
```

## 8. Data formats

| Data | Format |
|------|--------|
| Input | UTF-8 text files, one record per line. `map_fn` gets `(file path, line)`. |
| Intermediate | JSON Lines. Each line is a two-element array `[key, value]`. Map task `m` writes R files, one per reduce task. |
| Output | Tab-separated text, `key<TAB>value`, one pair per line, sorted by key within each `part-NNNNN.txt`. |

A pair goes to reduce task `crc32(key) mod R`. CRC32 is used instead of
Python's built-in `hash` because `hash` gives different answers in different
processes.

JSON Lines was chosen for intermediate data because it is readable when
debugging, needs no extra dependency, and keeps value types (a count stays a
number).

## 9. Special feature: worker failure handling

### Detecting a dead worker

Every worker contacts the boss at least once a second: `GetTask` when idle,
`Heartbeat` while a task runs. A monitor thread in the boss declares a worker
dead after 5 seconds of silence. Both numbers are configurable.

A worker runs each task in a child process and keeps heartbeating from the
parent. CPU-heavy user code therefore cannot delay heartbeats and make a
healthy worker look dead.

### Recovering

- Each task is `idle`, `in progress` or `completed`, and has an attempt number
  that increases every time the task is handed out.
- When a worker is declared dead, the task it was running goes back to `idle`
  and the next worker that asks gets it.
- Completed tasks stay completed. In the original MapReduce paper, completed
  map tasks on a failed machine are re-run because their output was on that
  machine's local disk. Here the output is already in shared storage, so only
  the task that was running is lost.
- A dead worker that contacts the boss again is treated as a new idle worker.
- A worker that asks for a task while the boss still shows it running one
  must have restarted, so its old task is re-queued at once instead of
  waiting for the timeout.
- If the same task loses its worker three times, the boss fails the job. One
  task that crashes every worker cannot loop forever.

### Why the output stays correct

- **No partial files.** A task writes each file under a temporary name and
  renames it into place only when it is complete. A killed worker leaves
  behind temporary files that nobody reads.
- **No stale results.** `ReportTask` carries the attempt number. The boss
  accepts a report only for the current attempt of an in-progress task, so a
  late report from a worker that was presumed dead is ignored.
- **Re-running is harmless.** A re-run writes the same file names with the
  same content, because job functions are deterministic.
- **Reduce never sees half a map phase.** Reduce tasks are handed out only
  after every map task is complete.

### Errors in user code are not worker failures

If `map_fn` or `reduce_fn` raises an exception, the worker stays alive and
reports the traceback. The boss fails the job immediately and `status` shows
the traceback. Retrying would only hit the same bug again.

The same applies if the task's child process dies without raising, for
example because it is killed for using too much memory. The worker itself is
still alive, so it reports the task as failed.

### Approaches considered

| Approach | How it works | Trade-off |
|----------|--------------|-----------|
| **A. Pull with heartbeats (chosen)** | Workers ask for tasks and heartbeat; silence means dead. | Least code; one server; restarted workers rejoin for free. Detection takes up to the timeout. |
| B. Boss pushes tasks | Every worker is also a server; a failed call means dead. | Near-instant detection and closer to the paper, but two services, a boss thread per worker, and extra logic for returning workers. |
| C. Task deadline only | No heartbeats; re-queue any task that runs too long. | Cannot tell slow from dead, so long tasks get re-run for nothing and recovery is slow. |

A was chosen because it is the simplest design that detects failures
quickly, independent of how long tasks take.

## 10. Additional features

### 10.1 Combiners

A job file can define an optional third function:

```python
def combine_fn(key, values):
    # same shape as reduce_fn: one key, all the values this map task has for it
    yield key, sum(values)
```

If it is there, a map task does not write every pair that `map_fn` yields.
It collects the pairs by key in memory, calls `combine_fn` once per key, and
writes what that yields. For word count, a map task then writes one pair per
distinct word instead of one pair per word occurrence.

- To bound memory, the map task combines and writes out what it has collected
  every 100,000 pairs. A key can therefore be combined in several batches.
- The job's final result must be the same whether the combiner runs or not,
  and however its input is batched. That holds when the reduction can be done
  in any order and in pieces, such as a sum, a count, a maximum or a minimum.
  For those, `combine_fn` can do exactly what `reduce_fn` does.
- Keys that `combine_fn` yields must be strings. Each pair is assigned to a
  reduce task by the key the combiner yields.

The combiner runs inside the map task, not as a separate step, so no file is
read or written twice.

### 10.2 Largest reduce task first

The first performance evaluation found a weakness. Reduce tasks were handed
out in numerical order, and the one with the most input happened to be
started in the second round. The job then waited for it alone at the end.

Now, when the last map task of a job completes, the boss adds up the size of
the intermediate files of each reduce task and hands the reduce tasks out
from largest to smallest. The largest task starts first, and smaller ones
fill in around it.

- The cost is one size lookup per intermediate file (M times R of them), once
  per job.
- The sizes also give the total amount of intermediate data, which `status`
  now reports. The combiner experiment uses this number.
- `--reduce-order numerical` restores the old order for one job.

### 10.3 Backup tasks for stragglers

A slow worker holds up a whole job at the end of each phase, because the job
has to wait for that worker's last task. A worker can be slow without being
dead: an overloaded machine, a failing disk, a throttled CPU.

**Rule.** When a worker asks for work and a job has no idle task to give it,
the boss looks for a task of that job that is running unusually slowly and
hands the worker a second copy of it, a backup. Whichever copy finishes first
completes the task. The other copy is cancelled (section 10.4).

**What counts as slow.** The boss knows the input size of every task: bytes
of input text for a map task, bytes of intermediate data for a reduce task.
It also knows how long each completed task took. From the completed tasks of
the same kind in the job it takes the median speed, in seconds per byte. A
running task is a straggler when it has already run 1.5 times longer than a
task of its size would take at that speed.

- At least three tasks of that kind must have completed, so there is
  something to compare with.
- A task gets at most one backup, and never on the worker already running it.

**Why judge by size.** Reduce tasks differ a lot in size: in the benchmark
one has 27% of the data. Comparing plain running times would mark that task
as a straggler in every run and waste a worker on duplicating it.

**Why the output stays correct.** Backups rely on the same three things as
failure handling. Each copy has its own attempt number and its own temporary
files. Files are renamed into place only when complete. Both copies produce
identical files, because job functions are deterministic. The boss accepts
the first report for a task and ignores the second.

If a worker dies while another copy of its task is still running, nothing is
re-queued: the surviving copy carries on.

`--no-backup-tasks` switches backups off for one job.

| Approach | How it works | Trade-off |
|----------|--------------|-----------|
| **Slow for its size, when a worker is free (chosen)** | As above. | Needs task sizes, which the boss has anyway. Does not duplicate a task that is merely big. |
| Duplicate everything still running near the end of a phase | What the original MapReduce paper describes. | Simplest. With uneven reduce tasks it duplicates healthy big tasks in every run. |
| Fixed time limit per task | Back up any task that runs longer than N seconds. | N would have to be tuned for every job and data size. |

### 10.4 Cancelling work that is no longer needed

The reply to `Heartbeat` can tell a worker that its task is no longer wanted.
That happens when another copy of the task has finished first, or when the
job has failed. The worker kills the task's child process and asks for new
work.

Without this, the loser of a backup race would run to the end for nothing,
and so would every task still running when a job fails. A cancelled task can
leave temporary files behind. Like those of a killed worker, they are never
read, and they are removed with the job's intermediate directory.

## 11. Tests

All tests use pytest. GitHub Actions runs them on every pull request.

**Unit tests** (no Docker, no network)

- Input splitting: every line is covered exactly once for many values of M,
  including lines that straddle a boundary, empty files, a missing final
  newline, and M larger than the input.
- Partitioning: stable and always in range.
- Map and reduce task code: word count on a temporary directory matches a
  reference computed in plain Python; no temporary files are left behind; a
  non-string key gives a clear error.
- Boss scheduling logic, driven with a fake clock: maps run before reduces; a
  silent worker's task is re-queued with a new attempt number; stale reports
  are ignored; a dead worker can rejoin; a user-code error fails the job; the
  three-losses limit works; jobs are served in order.

**Integration tests** (real gRPC on localhost, real worker processes)

- Word count with four workers matches the reference.
- One worker is killed with SIGKILL while it holds a task; the job still
  succeeds, the output matches the reference, and the log shows the re-run.
- All workers but one are killed; the job still succeeds.
- A killed worker is restarted under the same name; it rejoins and does more
  work.
- A worker is frozen until the boss presumes it dead, then resumed. It
  finishes its old task and reports it; the late report is ignored and the
  output is still correct.
- A healthy worker running a long CPU-bound task is not mistaken for a dead
  one.
- A job whose `map_fn` raises ends as failed with the traceback.

**Tests for the additional features**

- Combiners: the output with a combiner equals the output without one; the
  intermediate files hold far fewer pairs; a combiner that changes keys is
  partitioned correctly; batches are combined correctly when the batch limit
  is tiny.
- Reduce order, in the scheduler with a fake clock: reduce tasks come out
  largest first; the numerical option still works.
- Backup tasks, in the scheduler with a fake clock: no backup while an idle
  task exists; only a task that is slow for its size gets one; never on the
  same worker; never two; the first report completes the task and the second
  is ignored; a worker dying while the other copy runs does not re-queue the
  task; the heartbeat reply tells the loser to stop.
- Backup tasks with real worker processes: one worker is made slow; the job
  finishes sooner than the slow worker could manage, with correct output, and
  the slow worker's copy is cancelled.
- Cancelling: when a job fails, tasks still running on other workers are
  stopped.

**End-to-end test** (real Docker Compose cluster)

- Bring the cluster up, upload the sample data, submit word count,
  `docker kill` a worker mid-job, and check the output against the reference.
- Slow one worker container down with a CPU limit and check that a backup
  task ran and the output is correct.

## 12. Performance evaluation

Everything is measured on the real Compose cluster on one machine. The
workload is word count over 100 MB of generated text, with M = 16 and R = 8.

R is larger than the number of workers on purpose. Word frequencies are
skewed, so one reduce task has much more to do than the others. With more
reduce tasks than workers, the other workers have further tasks to take
while one of them is busy with the big one.

Each configuration is run several times (five for the first plot, four for
the second). The configurations are interleaved, and in the failure
experiment their order rotates from round to round, so that a machine that
slowly gets faster or slower affects all of them alike.
Job time is measured by the boss, from submission to the last reduce task
completing. One script runs the experiments and writes CSV files, and a
second draws the plots with matplotlib. The results are in
`docs/evaluation.md`.

**Plot 1: speedup with more workers.** Job time against the number of live
workers (1, 2, 3, 4): one dot per run and a line through the medians, plus a
dashed line for ideal speedup. This shows that tasks really are spread across
the 1-CPU workers.

**Plot 2: cost of a failure.** Bars for four cases: four workers with no
failure; one worker killed during the map phase; one worker killed during the
reduce phase; three workers from the start. Bars are medians, with one dot
per run. The expectation is that a run with a failure costs about as much as
having three workers, plus the detection delay. For every run the evaluation
also records whether its output is byte-identical to the failure-free output.

**Plot 3: timeline of one recovery.** One row per worker, one bar per task,
coloured by map or reduce. A vertical line marks the kill; the lost attempt is
hatched and its re-run on another worker is outlined. This is drawn from the
boss's `events.jsonl` and shows directly that the feature works as intended:
the dead worker's row stops, and its task reappears elsewhere after the
timeout.

The additional features get one measurement each, on the same workload.

**Plot 4: combiner.** Size of the intermediate data and job time, for word
count with and without `combine_fn`.

**Plot 5: reduce order.** Job time and length of the reduce phase, with
reduce tasks handed out in numerical order and largest first.

**Plot 6: backup tasks.** One worker is limited to a quarter of a CPU. Job
time with backup tasks on and off, next to the time with four healthy
workers, plus a timeline of one run that shows the duplicate being started
and the slow copy being cancelled.

## 13. Implementation notes

- One Docker image serves both boss and workers; the Compose file gives them
  different commands. The four workers are separate services with fixed names
  (`mr-worker-1` to `mr-worker-4`) and `cpus: 1`, so the demo can kill a
  specific one.
- Workers have no restart policy. A killed worker stays dead until it is
  started again, which keeps the demo and the measurements predictable.
- Generated protobuf code is not committed. The Docker build generates it for
  the containers, and the README gives the one command to generate it for the
  client.
- The repository includes a small sample dataset and a script that generates
  larger ones for the benchmarks.
- Left open for implementation: exact protobuf message fields, the job id
  format, polling intervals, and the benchmark dataset size.
