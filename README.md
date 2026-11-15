# MapReduce on Docker Compose

CS 544 Honors Project 1. A small MapReduce implementation in Python: one boss
and four workers run as containers under Docker Compose, each worker limited
to one CPU, and a command-line client on the host submits jobs.

**Special feature: worker failure handling.** If a worker dies in the middle
of a job, the boss notices within a few seconds, gives the lost task to
another worker, and the job finishes with exactly the output it would have
produced otherwise. [Try it](#7-try-the-special-feature-kill-a-worker) in
step 7.

**More features:** combiners, backup tasks for slow workers, and handing out
the largest reduce task first. See [step 8](#8-more-features).

- [design.md](design.md): how it works and why
- [docs/evaluation.md](docs/evaluation.md): performance measurements and plots

## What you need

- Docker with Compose v2 (`docker compose version`)
- Python 3.10 or later, for the client. On Ubuntu, creating the virtual
  environment in step 2 also needs `sudo apt install python3-venv`.

Run every command below from the root of this repository.

## 1. Build the Docker image

```bash
docker compose build
```

This builds one image, `mapreduce`, used by the boss and all four workers.
The gRPC code for the containers is generated inside the image.

## 2. Install the client

The client runs on the host, outside any container. It needs gRPC and the
generated protobuf code:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. mapreduce/mapreduce.proto
```

## 3. Start the cluster

```bash
docker compose up -d
python -m mapreduce.client workers
```

After a second or two the second command lists four live workers:

```
worker-1  alive  idle
worker-2  alive  idle
worker-3  alive  idle
worker-4  alive  idle
```

Shared storage is the `shared/` directory of this repository. It is mounted
into every container, so the containers and the client all see the same
files.

## 4. Upload the example data

`data/sample` holds four small text files (about 400 KB in total).

```bash
python -m mapreduce.client upload data/sample input/sample
```

```
uploaded data/sample to shared/input/sample (4 files)
```

The second argument is a path inside shared storage. `upload` works for a
single file or a whole directory.

## 5. Write a job

A job is one Python file that defines two generator functions, `map_fn` and
`reduce_fn`. This is the complete word count job,
[examples/wordcount.py](examples/wordcount.py):

```python
def map_fn(key, value):
    """key is the path of the input file, value is one line of text."""
    for word in value.split():
        yield word.lower(), 1


def reduce_fn(key, values):
    """key is a word, values iterates over every count emitted for it."""
    yield key, sum(values)
```

- `map_fn` is called once for every line of every input file. It yields any
  number of `(key, value)` pairs.
- `reduce_fn` is called once for every distinct key, with all the values that
  were yielded for it. It yields any number of `(key, value)` pairs, which
  are written to the output as `key<TAB>value` lines.

Rules:

- Keys yielded by `map_fn` must be strings. Values can be anything JSON can
  represent (numbers, strings, lists, dictionaries).
- A job file can import only the Python standard library.
- Both functions should be deterministic. A task can run twice after a
  failure, and both runs must produce the same output.

### Optional: a combiner

A job file can define a third function, `combine_fn`. Each map task then
collects its own output by key and passes it through `combine_fn` before
writing it, so far less data goes to the reduce tasks.
[examples/wordcount_combiner.py](examples/wordcount_combiner.py) is word count
with this added:

```python
def combine_fn(key, values):
    """Runs inside a map task: key is a word, values are the counts that
    this map task has collected for it so far."""
    yield key, sum(values)
```

Submit it like any other job (`--job examples/wordcount_combiner.py`, with a
new `--output`). The result is the same as without the combiner. On the
sample data the intermediate data shrinks from 1.1 MB to 34.9 KB.

A map task combines its output in batches of 100,000 pairs, so a key can be
combined more than once. What `combine_fn` yields is then passed to
`reduce_fn`. A combiner is therefore only correct if reducing combined pieces
gives the same result as reducing the raw values:

- For a sum, a maximum or a minimum, `combine_fn` can do the same as
  `reduce_fn`.
- A reducer that counts its values (`len(list(values))`) cannot be reused as
  the combiner: it would count the pieces. Emit 1 from `map_fn` and sum, as
  word count does.
- For an average, the combiner must pass on a sum and a count, and only the
  reducer divides.

A job file can also say `combine_fn = None`, which is the same as leaving it
out.

## 6. Submit the job

```bash
python -m mapreduce.client submit \
    --input input/sample \
    --output output/wordcount \
    --job examples/wordcount.py \
    --maps 8 \
    --reduces 4
```

| Option | Meaning |
|--------|---------|
| `--input` | input file or directory in shared storage |
| `--output` | output directory in shared storage; it must not already contain files |
| `--job` | local Python file with `map_fn` and `reduce_fn` |
| `--maps` | number of map tasks |
| `--reduces` | number of reduce tasks |
| `--no-wait` | optional: print the job id and return at once |
| `--reduce-order` | optional. Reduce tasks with the most input are handed out first, so that the one the job waits for longest starts early. `numerical` hands them out in order of task number instead (see [step 8](#8-more-features)) |
| `--no-backup-tasks` | optional: see [step 8](#8-more-features) |

The client shows progress until the job finishes:

```
submitted job-20261115-194834-ffde
map 0/8  reduce 0/4  0.0s
map 8/8  reduce 2/4  0.5s
map 8/8  reduce 4/4  0.6s
SUCCEEDED in 0.6s
intermediate data: 1.1 MB
output: shared/output/wordcount
```

"Intermediate data" is the total size of what the map tasks wrote for the
reduce tasks. There is one output file per reduce task. To see the ten most common words:

```bash
cat shared/output/wordcount/part-*.txt | sort -t$'\t' -k2 -nr | head
```

Other client commands:

```bash
python -m mapreduce.client status <job-id>     # progress of one job
python -m mapreduce.client workers             # the workers and what each is doing
python -m mapreduce.client submit ... --no-wait   # print the job id and return at once
```

If a job's code raises an exception, the job fails and the client prints the
traceback. A failed job can leave some part files behind, so delete its output
directory (or choose another `--output`) before submitting again.

## 7. Try the special feature: kill a worker

The sample data is processed in under a second, which is too quick to
interrupt. Generate and upload a larger dataset (about 100 MB; the job takes
roughly 20 to 30 seconds):

```bash
python scripts/gen_data.py --out data/bench --size-mb 100 --files 8
python -m mapreduce.client upload data/bench input/bench
```

In one terminal, submit the job:

```bash
python -m mapreduce.client submit --input input/bench --output output/with-failure \
    --job examples/wordcount.py --maps 16 --reduces 8
```

In a second terminal, while the job is running, kill a worker:

```bash
docker kill mr-worker-3
python -m mapreduce.client workers
```

About five seconds later the boss declares `worker-3` dead and hands its task
to another worker. The job still finishes, and the client reports the re-run:

```
SUCCEEDED in 23.2s (1 task re-run after a worker failure)
```

The boss's log shows what happened (`docker compose logs boss`):

```
boss: worker worker-3 has gone silent and is presumed dead
boss: job job-...: map task 6 was lost with worker worker-3 and is back in the queue
```

Bring the worker back; it rejoins by itself:

```bash
docker start mr-worker-3
```

To confirm that the failure did not change the result, run the same job
again without killing anything and compare the two outputs:

```bash
python -m mapreduce.client submit --input input/bench --output output/no-failure \
    --job examples/wordcount.py --maps 16 --reduces 8
diff <(cat shared/output/with-failure/part-*) <(cat shared/output/no-failure/part-*) && echo identical
```

## 8. More features

**Combiners.** A job file can define `combine_fn` to shrink the data that map
tasks pass to reduce tasks. See
[Optional: a combiner](#optional-a-combiner) in step 5.

**Largest reduce task first.** When the map phase ends, the boss measures how
much input each reduce task has and hands out the largest first, so the task
the job has to wait for longest starts as early as possible. Nothing needs to
be switched on. `submit --reduce-order numerical` goes back to handing them
out in order of task number.

**Backup tasks for slow workers.** If a task is running much more slowly than
its size explains, and a worker has nothing else to do, the boss gives that
worker a second copy of the task. Whichever copy finishes first completes the
task and the other is stopped. To try it, slow one worker down and run the
100 MB job from step 7:

```bash
docker update --cpus 0.25 mr-worker-3      # this worker now gets a quarter of a CPU
python -m mapreduce.client submit --input input/bench --output output/slow-worker \
    --job examples/wordcount.py --maps 16 --reduces 8
docker update --cpus 1 mr-worker-3         # back to normal
```

```
SUCCEEDED in 19.0s (1 backup task started for slow tasks)
```

The boss's log names the task (`docker compose logs boss`):

```
boss: job job-...: reduce task 4 is running slowly, so worker worker-2 gets a backup copy
```

With `--no-backup-tasks` (and a new `--output`) the same job has to wait for
the slow worker. On the machine used for the evaluation it took 28.6 seconds.

## Running the tests

```bash
pip install -r requirements-dev.txt
python -m pytest               # unit and integration tests; no Docker needed
python -m pytest -m docker     # end-to-end tests on the Compose cluster
```

The first command runs about 100 tests in under a minute. They include
integration tests that start a real boss and real worker processes, then
kill, restart and freeze workers in the middle of a job. The second command builds and starts the
Compose cluster if it is not already running, and includes a test that runs
`docker kill` on a worker during a job.

GitHub Actions runs both on every pull request
([.github/workflows/tests.yml](.github/workflows/tests.yml)).

## Performance evaluation

[docs/evaluation.md](docs/evaluation.md) has the measurements and plots. They
were produced with these commands, which take about 45 minutes and stop and
kill worker containers as they go:

```bash
pip install -r requirements-dev.txt
python bench/run_benchmarks.py --only scaling --trials 5 --cooldown 25
python bench/run_benchmarks.py --only failure --trials 4 --cooldown 45
python bench/run_benchmarks.py --only timeline --cooldown 25
python bench/plot.py
```

`--cooldown` pauses before each run. The measurements were taken on a
fanless laptop that slows down when it gets hot. On a machine with proper
cooling, `python bench/run_benchmarks.py` runs all three experiments without
pauses in about 15 minutes.

## Stopping and cleaning up

```bash
docker compose down                            # stop the cluster
rm -rf shared/input shared/output shared/jobs  # empty shared storage
```

## Settings

| Setting | Default | Meaning |
|---------|---------|---------|
| `MR_HEARTBEAT_TIMEOUT` (environment variable read by `docker compose up`) | `5` | seconds of silence after which the boss presumes a worker dead; keep it well above the 1 second between heartbeats |
| `--boss` (client option) or `MR_BOSS` | `localhost:50051` | address of the boss |
| `--shared` (client option) or `MR_SHARED_DIR` | `shared` | the shared storage directory on the host |

The boss's port is published on `localhost` only. The boss runs whatever job
code it is sent, so it should not be reachable from the network.

## Repository layout

```
design.md               design document
docker-compose.yml      boss and four workers (1 CPU each)
Dockerfile              one image for boss and workers
mapreduce/
  mapreduce.proto       the gRPC service
  boss.py               gRPC server: accepts jobs, hands out tasks
  scheduler.py          task and job state, failure detection and recovery
  worker.py             asks for tasks, runs them, sends heartbeats
  tasks.py              what a map task and a reduce task do
  splits.py             cutting the input into one piece per map task
  storage.py            where things live in shared storage
  client.py             the command-line client
examples/wordcount.py   example job
data/sample/            example data
scripts/gen_data.py     generates larger datasets
tests/                  unit, integration and end-to-end tests
bench/                  benchmark scripts, results and plots
docs/evaluation.md      performance evaluation
shared/                 shared storage (mounted into every container)
```
