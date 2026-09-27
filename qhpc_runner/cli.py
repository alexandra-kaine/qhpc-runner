from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import argparse
import fcntl
import sys

from .config import load_experiment
from .db import Database
from .experiment import ExperimentRunner
from .scheduler import SlurmScheduler
from . import report as reporting


def repo_root_from_config(config_path: Path) -> Path:
    p = config_path.resolve()
    # Example configs live under examples/<name>/; repository root is the first
    # ancestor containing pyproject.toml.
    for parent in [p.parent, *p.parents]:
        if (parent / "pyproject.toml").exists():
            return parent
    return Path.cwd().resolve()


def db_path(repo_root: Path) -> Path:
    return repo_root / ".qhpc_runner" / "state.sqlite3"


@contextmanager
def exclusive_lock(repo_root: Path):
    """Allow only one state-changing command at a time.

    Two overlapping syncs could both see a task as READY and both submit it.
    The README could just say "don't do that"; the lock makes it impossible.
    """
    lock_path = repo_root / ".qhpc_runner" / "runner.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "w") as fh:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("another qhpc-run sync/resolve is already running here")
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)


def print_counts(db: Database, exp_id: int):
    counts = db.counts(exp_id)
    total = sum(counts.values())
    print(f"Tasks: {total}")
    preferred = [
        "READY", "RETRY_READY", "SUBMITTING", "PENDING", "RUNNING",
        "COMPLETED", "FAILED_TIMEOUT", "FAILED_OOM", "FAILED_OOM_SUSPECTED",
        "FAILED_APP", "FAILED_CANCELLED", "SUBMIT_ERROR", "ABANDONED"
    ]
    for key in preferred:
        if counts.get(key):
            print(f"  {key:<18} {counts[key]}")
    for key in sorted(set(counts) - set(preferred)):
        print(f"  {key:<18} {counts[key]}")


def cmd_plan(args):
    spec = load_experiment(args.config)
    root = repo_root_from_config(Path(args.config))
    with Database(db_path(root)) as db:
        runner = ExperimentRunner(root, db, SlurmScheduler())
        print("\n".join(runner.plan_lines(spec)))


def cmd_sync(args):
    spec = load_experiment(args.config)
    root = repo_root_from_config(Path(args.config))
    with exclusive_lock(root), Database(db_path(root)) as db:
        runner = ExperimentRunner(root, db, SlurmScheduler())
        exp, messages = runner.sync(spec)
        print(f"Experiment: {exp['name']} [{exp['config_hash']}]")
        for line in messages:
            print(line)
        print_counts(db, exp["id"])


def cmd_status(args):
    spec = load_experiment(args.config)
    root = repo_root_from_config(Path(args.config))
    with Database(db_path(root)) as db:
        exp = db.experiment_by_hash(spec.config_hash)
        if exp is None:
            print("Experiment has not been submitted yet.")
            return
        print(f"Experiment: {exp['name']} [{exp['config_hash']}]")
        print(f"Created: {exp['created_at']}")
        print_counts(db, exp["id"])
        print()
        print(f"{'task':<22} {'status':<20} {'try':>3} {'job':<10} "
              f"{'slurm':<14} {'mem MB':>13} {'secs':>6}")
        for task in db.tasks_for_experiment(exp["id"]):
            last = db.latest_attempt(task["id"])
            jid = (last["slurm_job_id"] if last else None) or "-"
            sched = (last["scheduler_state"] if last else None) or "-"
            mem = "-"
            if last is not None:
                used = last["max_rss_mb"]
                mem = f"{used:.0f}/{last['memory_mb']}" if used is not None else f"-/{last['memory_mb']}"
            secs = (last["elapsed_sec"] if last else None)
            print(
                f"{task['task_key']:<22} {task['status']:<20} {task['attempt_count']:>3} "
                f"{jid:<10} {sched:<14} {mem:>13} {secs if secs is not None else '-':>6}"
            )
        stuck = [t["task_key"] for t in db.tasks_for_experiment(exp["id"])
                 if t["status"] in {"SUBMITTING", "SUBMIT_ERROR"}]
        if stuck:
            print()
            print("Needs attention: " + ", ".join(stuck))
            print("Check `squeue -a -u $USER` first, then: qhpc-run resolve <config> <task> --resubmit|--abandon")


def cmd_resolve(args):
    spec = load_experiment(args.config)
    root = repo_root_from_config(Path(args.config))
    with exclusive_lock(root), Database(db_path(root)) as db:
        runner = ExperimentRunner(root, db, SlurmScheduler())
        print(runner.resolve(spec, args.task, "resubmit" if args.resubmit else "abandon"))


def cmd_report(args):
    spec = load_experiment(args.config)
    root = repo_root_from_config(Path(args.config))
    with Database(db_path(root)) as db:
        exp = db.experiment_by_hash(spec.config_hash)
        if exp is None:
            print("Experiment has not been submitted yet.")
            return
        rows = reporting.collect(db, exp["id"])
        if not rows:
            print("No completed tasks with a QHPC_RESULT line yet.")
            return
        summary, axis = reporting.summarize(rows)
        out = root / "results" / f"{exp['name']}-{exp['config_hash']}"
        for path in reporting.write(out, exp["name"], rows, summary, axis):
            print(f"wrote {path}")
        print()
        print((out / "report.md").read_text())


def main():
    parser = argparse.ArgumentParser(
        prog="qhpc-run",
        description="Restart-safe Slurm experiment runner for QHPC",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    for name, func, help_text in [
        ("plan", cmd_plan, "preview the experiment without submitting jobs"),
        ("sync", cmd_sync, "reconcile with Slurm, retry eligible failures, and submit up to the cap"),
        ("status", cmd_status, "show persisted experiment state"),
        ("report", cmd_report, "summarize completed results (medians, speedup, chart)"),
    ]:
        p = sub.add_parser(name, help=help_text)
        p.add_argument("config", help="path to experiment YAML")
        p.set_defaults(func=func)

    p = sub.add_parser("resolve", help="manually settle a SUBMITTING or SUBMIT_ERROR task")
    p.add_argument("config", help="path to experiment YAML")
    p.add_argument("task", help="task key, as shown by status")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--resubmit", action="store_true", help="run it again as a new attempt")
    g.add_argument("--abandon", action="store_true", help="give up on this task")
    p.set_defaults(func=cmd_resolve)

    args = parser.parse_args()
    try:
        args.func(args)
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
