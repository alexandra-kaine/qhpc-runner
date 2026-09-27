from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
import getpass
import os
import subprocess


@dataclass
class JobRecord:
    job_id: str
    name: str
    state: str
    exit_code: str | None = None
    max_rss_mb: float | None = None   # peak memory of any step (batch step included)
    elapsed_sec: int | None = None


class SubmitError(RuntimeError):
    """sbatch ran and explicitly refused the job."""


class Scheduler:
    def submit(self, script_path: Path, job_name: str, extra_args: list[str]) -> str:
        raise NotImplementedError

    def snapshot(self, start_time: datetime) -> list[JobRecord]:
        raise NotImplementedError


def parse_mem_mb(value: str) -> float | None:
    """Parse a sacct memory value such as '512.25M', '1.5G', '2048K', or '0'."""
    value = (value or "").strip()
    if not value:
        return None
    factors = {"K": 1 / 1024, "M": 1.0, "G": 1024.0, "T": 1024.0 * 1024}
    suffix = value[-1].upper()
    try:
        if suffix in factors:
            return float(value[:-1]) * factors[suffix]
        return float(value)  # we request --units=M, so a bare number is MB
    except ValueError:
        return None


def parse_sacct(text: str) -> dict[str, JobRecord]:
    """Parse `sacct -n -P -o JobIDRaw,JobName,State,ExitCode,MaxRSS,ElapsedRaw`.

    Job lines ("12345") carry the job's name, state and exit code. Step lines
    ("12345.batch", "12345.0") carry the memory usage: MaxRSS is recorded per
    step, so the job line alone would always report no memory use.
    """
    jobs: dict[str, JobRecord] = {}
    step_rss: dict[str, float] = {}
    for line in text.splitlines():
        parts = line.split("|")
        if len(parts) < 6:
            continue
        job_id, name, state, exit_code, max_rss, elapsed = parts[:6]
        if not job_id:
            continue
        if "." in job_id:
            parent = job_id.split(".")[0]
            rss = parse_mem_mb(max_rss)
            if rss is not None:
                step_rss[parent] = max(step_rss.get(parent, 0.0), rss)
            continue
        jobs[job_id] = JobRecord(
            job_id=job_id,
            name=name,
            state=state.split()[0].split("+")[0].upper() if state else "",
            exit_code=exit_code or None,
            elapsed_sec=int(elapsed) if elapsed.isdigit() else None,
        )
    for job_id, rss in step_rss.items():
        if job_id in jobs:
            jobs[job_id].max_rss_mb = round(rss, 1)
    return jobs


def parse_squeue(text: str) -> dict[str, JobRecord]:
    """Parse `squeue -a -h -o %i|%j|%T`."""
    jobs: dict[str, JobRecord] = {}
    for line in text.splitlines():
        parts = line.split("|")
        if len(parts) < 3:
            continue
        job_id, name, state = parts[:3]
        jobs[job_id] = JobRecord(job_id, name, state.upper(), None)
    return jobs


class SlurmScheduler(Scheduler):
    def submit(self, script_path: Path, job_name: str, extra_args: list[str]) -> str:
        cmd = ["sbatch", "--parsable", f"--job-name={job_name}", *extra_args, str(script_path)]
        p = subprocess.run(cmd, capture_output=True, text=True)
        if p.returncode != 0:
            raise SubmitError(p.stderr.strip() or f"sbatch exited {p.returncode}")
        # --parsable prints JOBID or JOBID;cluster
        return p.stdout.strip().split(";")[0]

    def snapshot(self, start_time: datetime) -> list[JobRecord]:
        # One accounting query covering the experiment's lifetime. -S matters:
        # without it sacct only reports jobs since midnight, and a job that
        # finished while the runner was down yesterday would be invisible.
        start = (start_time - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%S")
        records: dict[str, JobRecord] = {}
        p = subprocess.run(
            ["sacct", "-S", start, "-n", "-P", "--units=M",
             "-o", "JobIDRaw,JobName,State,ExitCode,MaxRSS,ElapsedRaw"],
            capture_output=True, text=True,
        )
        if p.returncode == 0:
            records.update(parse_sacct(p.stdout))

        # squeue is fresher for queued/running jobs (accounting can lag a few seconds).
        user = os.environ.get("USER") or getpass.getuser()
        q = subprocess.run(["squeue", "-a", "-h", "-u", user, "-o", "%i|%j|%T"],
                           capture_output=True, text=True)
        if q.returncode == 0:
            for job_id, rec in parse_squeue(q.stdout).items():
                if job_id in records:
                    records[job_id].state = rec.state
                else:
                    records[job_id] = rec
        return list(records.values())


class FakeScheduler(Scheduler):
    """Small deterministic scheduler used by the unit tests."""

    def __init__(self):
        self.records: dict[str, JobRecord] = {}
        self.submissions: list[tuple[str, str]] = []
        self.next_id = 1000
        self.fail_next_submit: str | None = None

    def submit(self, script_path: Path, job_name: str, extra_args: list[str]) -> str:
        if self.fail_next_submit:
            msg, self.fail_next_submit = self.fail_next_submit, None
            raise SubmitError(msg)
        self.next_id += 1
        jid = str(self.next_id)
        self.records[jid] = JobRecord(jid, job_name, "PENDING", None)
        self.submissions.append((jid, job_name))
        return jid

    def snapshot(self, start_time: datetime) -> list[JobRecord]:
        return [JobRecord(**vars(r)) for r in self.records.values()]

    def set_state(self, job_id: str, state: str, exit_code: str | None = None,
                  max_rss_mb: float | None = None, elapsed_sec: int | None = None):
        rec = self.records[job_id]
        self.records[job_id] = JobRecord(rec.job_id, rec.name, state, exit_code,
                                         max_rss_mb, elapsed_sec)

    def inject(self, record: JobRecord):
        self.records[record.job_id] = record
