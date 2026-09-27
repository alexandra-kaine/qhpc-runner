# qhpc-runner

`qhpc-runner` is a small, restart-safe experiment runner for Slurm.

QHPC is beginning to build benchmark and competition workflows on Queen's Frontenac cluster.
Even a simple scaling study quickly becomes a collection of jobs with different resource
requests, repeated runs, partial failures, and results that are easy to separate from the
configuration that produced them.

The runner keeps that experiment state in SQLite, advances it through short `sync` calls, and
reconciles its local state against Slurm rather than assuming an always-running controller
process.

## What it does

- expands a YAML experiment definition into deterministic tasks;
- persists experiments, tasks, attempts, and submission tokens in SQLite;
- submits work through `sbatch` with an explicit in-flight cap;
- reconciles active and completed jobs using `squeue` and `sacct`;
- recovers a job submitted immediately before the runner itself crashed;
- retries configured `TIMEOUT` and `OUT_OF_MEMORY` failures within strict limits;
- recognises a *suspected* out-of-memory kill from evidence (SIGKILL at the memory limit) when a
  cluster reports it as a plain `FAILED`;
- never automatically retries ordinary application failures;
- stops cleanly when `sbatch` rejects a job, instead of leaving it half-submitted;
- refuses to run two state-changing commands at once (file lock);
- records per-attempt peak memory and runtime from Slurm accounting;
- turns results into medians, speedup, efficiency, and a chart (`report`);
- exits after every `sync` rather than staying resident on a login node.

The first real workload is an OpenMP scaling study. A separate failure demo exists to exercise
the recovery logic without contaminating the benchmark.

## Architecture

```text
 experiment.yaml
       |
       v
 +-------------+
 | qhpc-run    |
 | plan / sync |
 | status      |
 +------+------+
        |
   +----+------------------+
   |                       |
   v                       v
 SQLite                 Slurm
 experiment             sbatch
 state                   squeue
                         sacct
                           |
                           v
                       Frontenac
```

SQLite and Slurm are independent sources of state. The interesting part of the project is
reconciling them safely when either the jobs or the runner can be interrupted.

## Install

On Frontenac:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
make -C examples/openmp
```

Check that the cluster commands are available:

```bash
which sbatch squeue sacct scontrol
scontrol ping
```

## OpenMP scaling example

Preview the experiment without submitting anything:

```bash
qhpc-run plan examples/openmp/experiment.yaml
```

The matrix runs 1, 2, 4, and 8 OpenMP threads with three repeats each.

Submit/reconcile up to the configured cap:

```bash
qhpc-run sync examples/openmp/experiment.yaml
```

Check persisted state:

```bash
qhpc-run status examples/openmp/experiment.yaml
```

Run `sync` again later to reconcile completed jobs and submit the next set.

When everything has completed:

```bash
python -m pip install -e ".[report]"   # matplotlib, for the chart
qhpc-run report examples/openmp/experiment.yaml
```

This writes `results/<experiment>-<hash>/` containing `raw.csv`, `summary.csv`, `report.md`,
and `scaling.png`. Repeats are summarised by their median, and the `spread_pct` column shows how
far apart the repeats were: a large spread means the timing is noisy and the median deserves
less trust.

## Results

<!-- Replace this with your real Frontenac output: the scaling.png chart, the summary table
     from report.md, and two or three sentences on what you see (for example, where efficiency
     drops and why). Do not paste numbers from anywhere except your own run. -->

The workload sets:

```text
OMP_NUM_THREADS={threads}
OMP_PROC_BIND=close
OMP_PLACES=cores
```

so the requested Slurm CPUs and the OpenMP runtime agree on thread count and placement.

The initial timing results should be treated as exploratory: Frontenac is shared infrastructure,
so node placement and concurrent activity can add noise. Compare repeated runs rather than
over-interpreting one timing.

## Failure/recovery demo

The failure experiment contains four small tasks:

| task | expected behaviour |
| --- | --- |
| `success` | completes normally |
| `app-error` | exits non-zero and is not retried |
| `timeout` | exceeds its walltime and is eligible for one bounded retry |
| `oom` | deliberately exceeds a small memory request and is eligible for one bounded retry if Slurm reports `OUT_OF_MEMORY` |

Preview it:

```bash
qhpc-run plan examples/failure-demo/experiment.yaml
```

### Deliberate crash immediately after submission

For the recovery demo only, the runner contains an explicit failpoint:

```bash
QHPC_RUNNER_CRASH_AFTER_SBATCH=1 \
  qhpc-run sync examples/failure-demo/experiment.yaml
```

The process exits immediately after Slurm accepts one job but before its job ID is stored.

Run:

```bash
qhpc-run sync examples/failure-demo/experiment.yaml
```

again.

The task is already stored as `SUBMITTING` with a unique job name such as:

```text
qhpc-e1-t3-a1-7ac19e
```

`sync` searches both current queue state and Slurm accounting history, recovers the accepted
job by that submission token, and does not blindly submit a duplicate.

## Why `sync` instead of a daemon?

The runner intentionally does not stay alive on Frontenac's login environment. Slurm already
owns long-running computation. A `sync` invocation:

1. opens durable state;
2. reconciles it against the scheduler;
3. classifies finished attempts;
4. applies bounded retry rules;
5. submits only enough ready work to reach the in-flight cap;
6. persists changes;
7. exits.

That design makes restart recovery part of the normal execution model instead of an exceptional
case.

## Retry policy

Automatic retry is deliberately conservative.

| state | action |
| --- | --- |
| `COMPLETED` | persist success; never rerun |
| `TIMEOUT` | optional bounded retry with a larger walltime |
| `OUT_OF_MEMORY` | optional bounded retry with a larger memory request |
| suspected OOM | `FAILED`, killed by SIGKILL (exit 137 or signal 9), peak memory at 90%+ of the request; retried only with `include_suspected: true` |
| `FAILED` | application failure; no automatic retry |
| `CANCELLED` / `NODE_FAIL` / `PREEMPTED` | recorded; no automatic retry in the MVP |
| unknown | preserve evidence; do not guess |

The demo configuration caps attempts and resource growth. An OOM or timeout may indicate a
bad application, so adding resources is not treated as universally correct.

## Submission recovery

There is a small but important failure window:

```text
persist SUBMITTING + token
          |
          v
        sbatch
          |
          v
      Slurm accepts
          |
        CRASH
          |
          v
job ID was never persisted
```

The unique token is written before the external side effect and used as the Slurm job name.
On the next `sync`, the runner queries scheduler/accounting state from the experiment's start
time and reconnects the persisted task to the existing job.

If a `SUBMITTING` task cannot be found, the runner leaves it ambiguous rather than automatically
creating another copy, and `status` lists it under "Needs attention". After checking
`squeue -a -u $USER` yourself:

```bash
qhpc-run resolve examples/failure-demo/experiment.yaml <task> --resubmit   # or --abandon
```

A resubmitted task gets a fresh token, so a late-appearing copy of the old job can never be
mistaken for the new one. If the token matches more than one job, the runner also refuses to
guess and reports every match.

If `sbatch` itself rejects a job (wrong account, invalid partition), nothing reached the queue.
The runner marks that task `SUBMIT_ERROR` and stops submitting for this sync, since the same
error would almost certainly repeat for every task.

## Shared-cluster behaviour

Every experiment has `max_in_flight`. `PENDING`, `RUNNING`, and `SUBMITTING` tasks count
against that cap. The runner therefore does not treat a shared Slurm queue as an unlimited API.

The Slurm adapter also performs one accounting snapshot for the experiment time window per
`sync`, instead of issuing a separate `sacct` query for every task.

## Only one runner at a time

`sync` and `resolve` take an exclusive lock on `.qhpc_runner/runner.lock`. Two overlapping syncs
could both see the same task as ready and both submit it, so instead of documenting "don't do
that", the runner makes it impossible.

## Slurm behaviour worth knowing

Things this project had to handle that are easy to get wrong:
- Hidden Slurm partitions are omitted from the default `squeue` view on some clusters. The runner queries `squeue -a` so live jobs in those partitions are still visible during reconciliation.

- **Memory is reported per step.** In `sacct`, the job's own line has no `MaxRSS`; the
  `.batch` step line does. Reading only the job line reports zero memory for every job.
- **`sacct` only looks back to midnight by default.** Recovery queries pass `-S` with the
  experiment's start time, or a job that finished while the runner was down would be invisible.
- **Time limits are whole minutes.** A 90-second request becomes 2 minutes, and enforcement has
  some slack, so the runner rounds up explicitly and the timeout demo sleeps well past its limit.
- **Out-of-memory reporting depends on the cluster.** Where memory limits are enforced one way,
  Slurm says `OUT_OF_MEMORY`; elsewhere the job is simply killed and shows as `FAILED`.
- **Compile for the compute nodes, not the login node.** `-march=native` on a login node can
  produce a binary that dies with "Illegal instruction" on a compute node with a different CPU.
- **Editing the YAML starts a new experiment.** Experiments are identified by a hash of their
  configuration, so a changed resource request can never be silently mixed into old results.

## Why SQLite?

This is currently a single-user QHPC experiment tool. SQLite gives it transactional, durable
state without requiring another service to be installed or operated.

## Why not Snakemake, Nextflow, or Submitit?

Those are mature tools and better choices for many real workflows. This project is intentionally
narrow: it exists to explore experiment reproducibility, scheduler-state reconciliation,
idempotent submission, bounded failure recovery, and Slurm behaviour directly.

## Tests

The unit tests use a fake scheduler; CI does not need Frontenac.

They cover:

- in-flight submission cap, and the lock against concurrent syncs;
- completed work is never duplicated;
- recovery of a `SUBMITTING` task by its scheduler token, and refusing to guess on duplicates;
- manual resolution with a fresh token;
- bounded timeout and OOM retries, including the walltime ceiling;
- suspected-OOM detection, and that a low-memory kill is *not* treated as OOM;
- application errors are never retried; `sbatch` rejections stop submission;
- parsing real-shaped `sacct` output, including memory from the batch step;
- an earlier attempt is never re-classified after a retry (a regression test for a bug found
  when running against a real Slurm cluster);
- speedup and efficiency calculations in `report`.

Run:

```bash
python -m unittest discover -s tests -v
```

## Known limitations

- single-user experiment state; one runner should manage a given experiment database;
- not a claim of exactly-once execution under every scheduler/database/network failure;
- ambiguous submission state is preserved rather than automatically duplicated;
- suspected OOM is a heuristic; it is opt-in and reported with its evidence;
- retries are demonstration policies, not automatic resource optimization;
- no job arrays, DAG dependencies, or MPI workload support yet;
- `MaxRSS` is sampled by Slurm, so very short jobs may report no memory figure;
- OpenMP measurements on shared infrastructure should not be interpreted as controlled
  dedicated-node benchmark results.

## Roadmap

Useful next steps for QHPC:

1. add Slurm job-array support for homogeneous task matrices;
2. add MPI experiment support;
3. record environment metadata (compiler, modules, git commit) alongside results;
4. suggest resource requests from past `MaxRSS` and runtime instead of fixed multipliers;
5. add competition benchmark configurations as QHPC's 2027 preparation develops.
