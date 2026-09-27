from __future__ import annotations

from pathlib import Path
from datetime import datetime
import json
import os
import secrets
import shlex

from .config import ExperimentSpec
from .db import Database
from .scheduler import Scheduler, JobRecord, SubmitError


ACTIVE_STATES = {"PENDING", "RUNNING", "CONFIGURING", "COMPLETING", "SUSPENDED"}
TERMINAL_SUCCESS = {"COMPLETED"}
TERMINAL_TIMEOUT = {"TIMEOUT"}
TERMINAL_OOM = {"OUT_OF_MEMORY"}
TERMINAL_APP_FAIL = {"FAILED", "BOOT_FAIL", "DEADLINE", "REVOKED", "SPECIAL_EXIT"}
TERMINAL_CANCEL = {"CANCELLED", "PREEMPTED", "NODE_FAIL"}


def _safe_name(text: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "-_" else "-" for ch in text)[:48]


def slurm_minutes(seconds: int) -> int:
    """Slurm enforces time limits in whole minutes, so round up."""
    return max(1, -(-int(seconds) // 60))


def _time_hms(seconds: int) -> str:
    h, m = divmod(slurm_minutes(seconds), 60)
    return f"{h:02d}:{m:02d}:00"


def _killed(exit_code: str | None) -> bool:
    """True if Slurm's "code:signal" exit shows the job died from SIGKILL (the
    OOM killer's signal), either directly or as bash's 128+9 exit status."""
    if not exit_code or ":" not in exit_code:
        return False
    code, signal = exit_code.split(":", 1)
    return signal.strip() == "9" or code.strip() == "137"


class ExperimentRunner:
    def __init__(self, repo_root: Path, db: Database, scheduler: Scheduler):
        self.repo_root = repo_root.resolve()
        self.db = db
        self.scheduler = scheduler
        self.runtime = self.repo_root / ".qhpc_runner"
        self.job_dir = self.runtime / "jobs"
        self.log_dir = self.runtime / "logs"
        self.job_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

    def ensure_experiment(self, spec: ExperimentSpec):
        exp = self.db.experiment_by_hash(spec.config_hash)
        if exp is None:
            exp = self.db.create_experiment(spec)
        return exp

    def plan_lines(self, spec: ExperimentSpec) -> list[str]:
        lines = [
            f"Experiment: {spec.name}",
            f"Configuration hash: {spec.config_hash}",
            f"Tasks: {len(spec.tasks)}",
            f"Max in flight: {spec.max_in_flight}",
            "",
        ]
        for i, task in enumerate(spec.tasks, start=1):
            lines.append(
                f"{i:>3}. {task.key} | cpus={task.cpus_per_task} "
                f"mem={task.memory_mb}MB time={slurm_minutes(task.walltime_sec)}min"
            )
        return lines

    def _script_for(self, task, attempt, env: dict[str, str]) -> Path:
        token = attempt["submission_token"]
        path = self.job_dir / f"{token}.sbatch"
        log_path = Path(attempt["log_path"]).resolve()

        exports = []
        for key, value in env.items():
            exports.append(f"export {key}={shlex.quote(str(value))}")

        body = f"""#!/usr/bin/env bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task={attempt['cpus_per_task']}
#SBATCH --mem={attempt['memory_mb']}M
#SBATCH --time={_time_hms(attempt['walltime_sec'])}
#SBATCH --output={log_path}

set -euo pipefail
cd {shlex.quote(str(self.repo_root))}
{os.linesep.join(exports)}
echo "QHPC_RUNNER_TASK={task['task_key']}"
echo "QHPC_RUNNER_TOKEN={token}"
echo "QHPC_RUNNER_NODE=$(hostname)"
echo "QHPC_RUNNER_JOB_ID=${{SLURM_JOB_ID:-unknown}}"
{task['command']}
"""
        path.write_text(body)
        path.chmod(0o755)
        return path

    def _records_index(self, records: list[JobRecord]):
        by_id = {r.job_id: r for r in records}
        by_name: dict[str, list[JobRecord]] = {}
        for r in records:
            by_name.setdefault(r.name, []).append(r)
        return by_id, by_name

    def _classify(self, record: JobRecord, requested_mb: int) -> str:
        state = (record.state or "").upper().split("+")[0]
        if (
            state in TERMINAL_APP_FAIL
            and _killed(record.exit_code)
            and record.max_rss_mb is not None
            and record.max_rss_mb >= 0.9 * requested_mb
        ):
            # Killed with SIGKILL while at its memory limit. Some clusters report
            # this as FAILED rather than OUT_OF_MEMORY, depending on how memory
            # limits are enforced. Evidence-based, so it gets its own label.
            return "OOM_SUSPECTED"
        if state in ACTIVE_STATES:
            return "ACTIVE"
        if state in TERMINAL_SUCCESS:
            return "SUCCESS"
        if state in TERMINAL_TIMEOUT:
            return "TIMEOUT"
        if state in TERMINAL_OOM:
            return "OOM"
        if state in TERMINAL_CANCEL:
            return "CANCELLED"
        if state in TERMINAL_APP_FAIL:
            return "APP_ERROR"
        return "UNKNOWN"

    def _next_resources(self, spec: ExperimentSpec, task, last_attempt):
        resources = {
            "cpus_per_task": int(task["cpus_per_task"]),
            "memory_mb": int(task["memory_mb"]),
            "walltime_sec": int(task["walltime_sec"]),
        }
        failure = task["last_failure"]
        if last_attempt is not None:
            resources = {
                "cpus_per_task": int(last_attempt["cpus_per_task"]),
                "memory_mb": int(last_attempt["memory_mb"]),
                "walltime_sec": int(last_attempt["walltime_sec"]),
            }

        if failure == "TIMEOUT" and spec.timeout_retry:
            resources["walltime_sec"] = min(
                int(max(resources["walltime_sec"] + 1,
                        resources["walltime_sec"] * spec.timeout_retry.multiplier)),
                spec.timeout_retry.ceiling,
            )
        elif failure in {"OOM", "OOM_SUSPECTED"} and spec.oom_retry:
            resources["memory_mb"] = min(
                int(max(resources["memory_mb"] + 1,
                        resources["memory_mb"] * spec.oom_retry.multiplier)),
                spec.oom_retry.ceiling,
            )
        return resources

    def _retry_allowed(self, spec: ExperimentSpec, task, failure: str) -> bool:
        attempts_done = int(task["attempt_count"])
        if attempts_done >= spec.max_attempts:
            return False
        if failure == "TIMEOUT" and spec.timeout_retry:
            # attempts means number of retries allowed for this class
            return attempts_done <= spec.timeout_retry.attempts
        if failure == "OOM" and spec.oom_retry:
            return attempts_done <= spec.oom_retry.attempts
        if failure == "OOM_SUSPECTED" and spec.oom_retry and spec.oom_retry.include_suspected:
            return attempts_done <= spec.oom_retry.attempts
        return False

    def reconcile(self, spec: ExperimentSpec, exp) -> list[str]:
        messages: list[str] = []
        created_at = datetime.fromisoformat(exp["created_at"])
        records = self.scheduler.snapshot(created_at)
        by_id, by_name = self._records_index(records)

        for attempt in self.db.active_attempts(exp["id"]):
            record = None
            if attempt["slurm_job_id"]:
                record = by_id.get(str(attempt["slurm_job_id"]))
            if record is None:
                matches = by_name.get(attempt["submission_token"], [])
                if len(matches) == 1:
                    record = matches[0]
                    if attempt["task_status"] == "SUBMITTING":
                        messages.append(
                            f"RECOVERED {attempt['task_key']}: found job {record.job_id} "
                            f"by token {attempt['submission_token']}; not resubmitting"
                        )
                elif len(matches) > 1:
                    ids = ", ".join(r.job_id for r in matches)
                    messages.append(
                        f"WARNING {attempt['task_key']}: token matches several jobs ({ids}); "
                        f"leaving unchanged for manual review"
                    )
                    continue

            if record is None:
                # Important: never blindly resubmit an ambiguous SUBMITTING task.
                if attempt["task_status"] == "SUBMITTING":
                    messages.append(
                        f"WARNING {attempt['task_key']}: submission token "
                        f"{attempt['submission_token']} not found; preserving SUBMITTING. "
                        f"If squeue/sacct confirm no such job, run: qhpc-run resolve "
                        f"<config> {attempt['task_key']} --resubmit"
                    )
                continue

            self.db.update_from_scheduler(attempt["id"], record)

            cls = self._classify(record, int(attempt["memory_mb"]))
            task = self.db.task_by_id(attempt["task_id"])

            if cls == "ACTIVE":
                new_status = "RUNNING" if record.state == "RUNNING" else "PENDING"
                self.db.set_task_status(task["id"], new_status)
                continue

            if cls == "SUCCESS":
                self.db.finish_attempt(attempt["id"], record.state, record.exit_code)
                self.db.set_task_status(task["id"], "COMPLETED", None)
                messages.append(f"COMPLETED {task['task_key']} (job {record.job_id})")
                continue

            if cls in {"TIMEOUT", "OOM", "OOM_SUSPECTED"}:
                self.db.finish_attempt(attempt["id"], record.state, record.exit_code)
                if self._retry_allowed(spec, task, cls):
                    self.db.set_task_status(task["id"], "RETRY_READY", cls)
                    messages.append(f"RETRY {task['task_key']} after {cls}")
                else:
                    self.db.set_task_status(task["id"], f"FAILED_{cls}", cls)
                    messages.append(f"FAILED {task['task_key']} [{cls}]")
                if cls == "OOM_SUSPECTED":
                    messages.append(
                        f"  evidence: exit {record.exit_code}, peak {record.max_rss_mb} MB "
                        f"of {attempt['memory_mb']} MB requested"
                    )
                continue

            if cls == "APP_ERROR":
                self.db.finish_attempt(attempt["id"], record.state, record.exit_code)
                self.db.set_task_status(task["id"], "FAILED_APP", "APP_ERROR")
                messages.append(f"FAILED {task['task_key']} [APP_ERROR]")
                continue

            if cls == "CANCELLED":
                self.db.finish_attempt(attempt["id"], record.state, record.exit_code)
                self.db.set_task_status(task["id"], "FAILED_CANCELLED", cls)
                messages.append(f"FAILED {task['task_key']} [{record.state}]")
                continue

            messages.append(
                f"WARNING {task['task_key']}: unclassified Slurm state {record.state}; leaving unchanged"
            )

        return messages

    def submit_available(self, spec: ExperimentSpec, exp) -> list[str]:
        messages: list[str] = []
        active_count = len(self.db.active_tasks(exp["id"]))
        slots = max(0, int(spec.max_in_flight) - active_count)

        if slots == 0:
            return messages

        import json
        for task in self.db.ready_tasks(exp["id"])[:slots]:
            last = self.db.latest_attempt(task["id"])
            resources = self._next_resources(spec, task, last)
            attempt_no = int(task["attempt_count"]) + 1
            token = _safe_name(
                f"qhpc-e{exp['id']}-t{task['id']}-a{attempt_no}-{secrets.token_hex(3)}"
            )
            log_path = str((self.log_dir / f"{token}-%j.out").resolve())
            attempt = self.db.create_attempt(task, token, resources, log_path)
            env = json.loads(task["env_json"])
            script = self._script_for(task, attempt, env)

            try:
                job_id = self.scheduler.submit(script, token, spec.slurm_extra_args)
            except SubmitError as exc:
                # sbatch answered and refused (bad account, invalid request...).
                # Nothing reached the queue, so close this attempt and stop:
                # the same error would almost certainly repeat for every task.
                self.db.mark_attempt(attempt["id"], "SUBMIT_REJECTED")
                self.db.set_task_status(task["id"], "SUBMIT_ERROR", str(exc)[:500])
                messages.append(f"SUBMIT_ERROR {task['task_key']}: {exc}")
                messages.append("Stopped submitting. Fix the cause, then use `qhpc-run resolve`.")
                break

            # Deliberate failpoint for the crash-recovery demo.
            if os.environ.get("QHPC_RUNNER_CRASH_AFTER_SBATCH") == "1":
                os._exit(42)

            self.db.record_submission(attempt["id"], job_id)
            messages.append(
                f"SUBMITTED {task['task_key']} -> job {job_id} "
                f"(attempt {attempt_no})"
            )

        return messages

    def resolve(self, spec: ExperimentSpec, task_key: str, action: str) -> str:
        """Manual way out of a state the runner refuses to guess about."""
        exp = self.db.experiment_by_hash(spec.config_hash)
        if exp is None:
            raise ValueError("experiment has not been started")
        task = self.db.task_by_key(exp["id"], task_key)
        if task is None:
            raise ValueError(f"no task named {task_key!r}")
        if task["status"] not in {"SUBMITTING", "SUBMIT_ERROR"}:
            raise ValueError(
                f"{task_key} is {task['status']}; resolve only applies to "
                f"SUBMITTING or SUBMIT_ERROR tasks"
            )
        last = self.db.latest_attempt(task["id"])
        if last is not None and last["scheduler_state"] not in {"SUBMIT_REJECTED"}:
            self.db.mark_attempt(last["id"], "ABANDONED")
        if action == "resubmit":
            # The next attempt gets a fresh token, so a late-appearing copy of the
            # old job can never be confused with the new one.
            self.db.set_task_status(task["id"], "READY", None)
            return f"{task_key}: marked READY; the next sync submits a new attempt"
        self.db.set_task_status(task["id"], "ABANDONED", "manually abandoned")
        return f"{task_key}: abandoned"

    def sync(self, spec: ExperimentSpec) -> tuple[object, list[str]]:
        exp = self.ensure_experiment(spec)
        messages = []
        messages.extend(self.reconcile(spec, exp))
        messages.extend(self.submit_available(spec, exp))
        return exp, messages
