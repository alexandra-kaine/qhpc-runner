import json
import tempfile
import unittest
from unittest.mock import patch
from datetime import datetime
from pathlib import Path

from qhpc_runner.config import load_experiment
from qhpc_runner.db import Database
from qhpc_runner.experiment import ExperimentRunner
from qhpc_runner.scheduler import FakeScheduler, JobRecord, SlurmScheduler, parse_sacct
from qhpc_runner.experiment import slurm_minutes
from qhpc_runner import report as reporting
from qhpc_runner.cli import exclusive_lock


def write_config(path: Path, text: str):
    path.write_text(text)


BASE = """
name: test
resources:
  cpus_per_task: 1
  memory_mb: 64
  walltime_sec: 60
tasks:
{tasks}
execution:
  max_in_flight: {cap}
  max_attempts: 2
  retry:
    timeout:
      attempts: 1
      multiplier: 1.5
      max_seconds: 180
    out_of_memory:
      attempts: 1
      multiplier: 2.0
      max_memory_mb: 512
      include_suspected: {suspected}
slurm:
  extra_args: []
"""


class RunnerTests(unittest.TestCase):
    def make(self, tasks: str, cap=4, suspected="true"):
        td = tempfile.TemporaryDirectory()
        root = Path(td.name)
        (root / "pyproject.toml").write_text("[project]\nname='x'\nversion='0'\n")
        cfg = root / "exp.yaml"
        write_config(cfg, BASE.format(tasks=tasks, cap=cap, suspected=suspected))
        spec = load_experiment(cfg)
        db = Database(root / ".qhpc_runner/state.sqlite3")
        sched = FakeScheduler()
        runner = ExperimentRunner(root, db, sched)
        exp = runner.ensure_experiment(spec)
        return td, root, cfg, spec, db, sched, runner, exp

    def test_in_flight_cap(self):
        tasks = "\n".join(
            f"  - id: t{i}\n    command: echo {i}" for i in range(10)
        )
        td, root, cfg, spec, db, sched, runner, exp = self.make(tasks, cap=4)
        runner.submit_available(spec, exp)
        self.assertEqual(len(sched.submissions), 4)
        db.close(); td.cleanup()

    def test_completed_job_not_resubmitted(self):
        td, root, cfg, spec, db, sched, runner, exp = self.make(
            "  - id: one\n    command: echo ok"
        )
        runner.submit_available(spec, exp)
        jid = sched.submissions[0][0]
        sched.set_state(jid, "COMPLETED", "0:0")
        runner.reconcile(spec, exp)
        runner.submit_available(spec, exp)
        self.assertEqual(len(sched.submissions), 1)
        task = db.tasks_for_experiment(exp["id"])[0]
        self.assertEqual(task["status"], "COMPLETED")
        db.close(); td.cleanup()

    def test_crash_mid_submit_recovery_by_token(self):
        td, root, cfg, spec, db, sched, runner, exp = self.make(
            "  - id: one\n    command: echo ok"
        )
        task = db.tasks_for_experiment(exp["id"])[0]
        token = "qhpc-recovery-token"
        attempt = db.create_attempt(
            task, token,
            {"cpus_per_task": 1, "memory_mb": 64, "walltime_sec": 60},
            str(root / "log.out")
        )
        sched.inject(JobRecord("2001", token, "RUNNING", None))
        runner.reconcile(spec, exp)
        task2 = db.task_by_id(task["id"])
        attempt2 = db.latest_attempt(task["id"])
        self.assertEqual(task2["status"], "RUNNING")
        self.assertEqual(attempt2["slurm_job_id"], "2001")
        runner.submit_available(spec, exp)
        self.assertEqual(len(sched.submissions), 0)
        db.close(); td.cleanup()

    def test_bounded_oom_retry(self):
        td, root, cfg, spec, db, sched, runner, exp = self.make(
            "  - id: one\n    command: echo ok"
        )
        runner.submit_available(spec, exp)
        jid = sched.submissions[0][0]
        sched.set_state(jid, "OUT_OF_MEMORY", "0:9")
        runner.reconcile(spec, exp)
        runner.submit_available(spec, exp)
        self.assertEqual(len(sched.submissions), 2)

        jid2 = sched.submissions[1][0]
        sched.set_state(jid2, "OUT_OF_MEMORY", "0:9")
        runner.reconcile(spec, exp)
        runner.submit_available(spec, exp)
        self.assertEqual(len(sched.submissions), 2)
        task = db.tasks_for_experiment(exp["id"])[0]
        self.assertEqual(task["status"], "FAILED_OOM")
        db.close(); td.cleanup()

    def test_app_error_never_retries(self):
        td, root, cfg, spec, db, sched, runner, exp = self.make(
            "  - id: one\n    command: exit 7"
        )
        runner.submit_available(spec, exp)
        jid = sched.submissions[0][0]
        sched.set_state(jid, "FAILED", "7:0")
        runner.reconcile(spec, exp)
        runner.submit_available(spec, exp)
        self.assertEqual(len(sched.submissions), 1)
        task = db.tasks_for_experiment(exp["id"])[0]
        self.assertEqual(task["status"], "FAILED_APP")
        db.close(); td.cleanup()

    # ---- added in 0.2 -------------------------------------------------

    def test_slurm_snapshot_includes_hidden_partitions(self):
        """Live jobs on hidden Frontenac partitions must be included in squeue."""
        class Result:
            def __init__(self, stdout=""):
                self.returncode = 0
                self.stdout = stdout
                self.stderr = ""

        with patch("qhpc_runner.scheduler.subprocess.run") as run:
            run.side_effect = [
                Result(""),
                Result("9001|qhpc-hidden-test|RUNNING\n"),
            ]

            records = SlurmScheduler().snapshot(
                datetime.fromisoformat("2026-09-26T19:00:00")
            )

        squeue_cmd = run.call_args_list[1].args[0]
        self.assertEqual(squeue_cmd[:3], ["squeue", "-a", "-h"])
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].job_id, "9001")
        self.assertEqual(records[0].state, "RUNNING")

    def test_sacct_parsing_reads_memory_from_batch_step(self):
        # Shaped like real `sacct -n -P --units=M` output: the job line has no
        # MaxRSS; the batch/extern steps do.
        text = (
            "5001|qhpc-e1-t4-a1-abc123|OUT_OF_MEMORY|0:125||62\n"
            "5001.batch|batch|OUT_OF_MEMORY|0:125|63.85M|62\n"
            "5001.extern|extern|COMPLETED|0:0|0.10M|62\n"
            "5002|qhpc-e1-t5-a1-def456|CANCELLED by 1234|0:0||5\n"
        )
        jobs = parse_sacct(text)
        self.assertEqual(jobs["5001"].state, "OUT_OF_MEMORY")
        self.assertAlmostEqual(jobs["5001"].max_rss_mb, 63.85, delta=0.1)
        self.assertEqual(jobs["5001"].elapsed_sec, 62)
        self.assertEqual(jobs["5002"].state, "CANCELLED")

    def test_suspected_oom_is_retried_only_when_enabled(self):
        for enabled, expected_submissions, expected_status in [
            ("true", 2, "RETRY_READY"), ("false", 1, "FAILED_OOM_SUSPECTED")
        ]:
            td, root, cfg, spec, db, sched, runner, exp = self.make(
                "  - id: one\n    command: echo ok", suspected=enabled
            )
            runner.submit_available(spec, exp)
            jid = sched.submissions[0][0]
            # FAILED, exit 137 (bash's 128+SIGKILL), peak memory at the 64 MB limit
            sched.set_state(jid, "FAILED", "137:0", max_rss_mb=63.9)
            msgs = runner.reconcile(spec, exp)
            task = db.tasks_for_experiment(exp["id"])[0]
            self.assertEqual(task["status"], expected_status, msgs)
            runner.submit_available(spec, exp)
            self.assertEqual(len(sched.submissions), expected_submissions)
            db.close(); td.cleanup()

    def test_plain_failure_with_low_memory_is_app_error(self):
        td, root, cfg, spec, db, sched, runner, exp = self.make(
            "  - id: one\n    command: echo ok"
        )
        runner.submit_available(spec, exp)
        jid = sched.submissions[0][0]
        sched.set_state(jid, "FAILED", "137:0", max_rss_mb=5.0)  # killed, but not near the limit
        runner.reconcile(spec, exp)
        self.assertEqual(db.tasks_for_experiment(exp["id"])[0]["status"], "FAILED_APP")
        db.close(); td.cleanup()

    def test_sbatch_rejection_stops_submitting(self):
        tasks = "\n".join(f"  - id: t{i}\n    command: echo {i}" for i in range(3))
        td, root, cfg, spec, db, sched, runner, exp = self.make(tasks)
        sched.fail_next_submit = "Invalid account or account/partition combination"
        msgs = runner.submit_available(spec, exp)
        self.assertEqual(len(sched.submissions), 0)
        statuses = [t["status"] for t in db.tasks_for_experiment(exp["id"])]
        self.assertEqual(statuses, ["SUBMIT_ERROR", "READY", "READY"])
        self.assertTrue(any("SUBMIT_ERROR" in m for m in msgs))
        db.close(); td.cleanup()

    def test_resolve_resubmits_with_fresh_token(self):
        td, root, cfg, spec, db, sched, runner, exp = self.make(
            "  - id: one\n    command: echo ok"
        )
        task = db.tasks_for_experiment(exp["id"])[0]
        db.create_attempt(task, "qhpc-lost-token",
                          {"cpus_per_task": 1, "memory_mb": 64, "walltime_sec": 60},
                          str(root / "log.out"))
        runner.reconcile(spec, exp)                      # token not found anywhere
        self.assertEqual(db.task_by_id(task["id"])["status"], "SUBMITTING")
        runner.resolve(spec, "one", "resubmit")
        runner.submit_available(spec, exp)
        self.assertEqual(len(sched.submissions), 1)
        self.assertNotEqual(sched.submissions[0][1], "qhpc-lost-token")
        first = db.attempts_for_task(task["id"])[0]
        self.assertEqual(first["scheduler_state"], "ABANDONED")
        db.close(); td.cleanup()

    def test_duplicate_token_matches_are_not_guessed(self):
        td, root, cfg, spec, db, sched, runner, exp = self.make(
            "  - id: one\n    command: echo ok"
        )
        task = db.tasks_for_experiment(exp["id"])[0]
        db.create_attempt(task, "qhpc-dup",
                          {"cpus_per_task": 1, "memory_mb": 64, "walltime_sec": 60},
                          str(root / "log.out"))
        sched.inject(JobRecord("3001", "qhpc-dup", "RUNNING"))
        sched.inject(JobRecord("3002", "qhpc-dup", "RUNNING"))
        msgs = runner.reconcile(spec, exp)
        self.assertEqual(db.task_by_id(task["id"])["status"], "SUBMITTING")
        self.assertTrue(any("several jobs" in m for m in msgs))
        db.close(); td.cleanup()

    def test_timeout_retry_grows_walltime_up_to_ceiling(self):
        td, root, cfg, spec, db, sched, runner, exp = self.make(
            "  - id: one\n    command: echo ok\n    resources:\n      walltime_sec: 150"
        )
        runner.submit_available(spec, exp)
        sched.set_state(sched.submissions[0][0], "TIMEOUT", "0:0")
        runner.reconcile(spec, exp)
        runner.submit_available(spec, exp)
        second = db.latest_attempt(db.tasks_for_experiment(exp["id"])[0]["id"])
        self.assertEqual(second["walltime_sec"], 180)   # 150 * 1.5 = 225, capped at 180
        db.close(); td.cleanup()

    def test_old_attempt_is_not_reclassified_after_retry(self):
        # Regression test for a bug found on a real Slurm run: once attempt 2
        # was running, every sync re-read attempt 1's TIMEOUT and flipped the
        # task to FAILED_TIMEOUT and back.
        td, root, cfg, spec, db, sched, runner, exp = self.make(
            "  - id: one\n    command: echo ok"
        )
        runner.submit_available(spec, exp)
        sched.set_state(sched.submissions[0][0], "TIMEOUT", "0:0")
        runner.reconcile(spec, exp)
        runner.submit_available(spec, exp)
        sched.set_state(sched.submissions[1][0], "RUNNING")
        for _ in range(3):
            msgs = runner.reconcile(spec, exp)
            self.assertFalse(any("FAILED" in m for m in msgs), msgs)
            self.assertEqual(db.tasks_for_experiment(exp["id"])[0]["status"], "RUNNING")
        sched.set_state(sched.submissions[1][0], "COMPLETED", "0:0")
        runner.reconcile(spec, exp)
        self.assertEqual(db.tasks_for_experiment(exp["id"])[0]["status"], "COMPLETED")
        db.close(); td.cleanup()

    def test_walltime_rounds_up_to_whole_minutes(self):
        self.assertEqual(slurm_minutes(1), 1)
        self.assertEqual(slurm_minutes(60), 1)
        self.assertEqual(slurm_minutes(90), 2)

    def test_lock_blocks_a_second_sync(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            with exclusive_lock(root):
                with self.assertRaises(RuntimeError):
                    with exclusive_lock(root):
                        pass
            with exclusive_lock(root):   # released afterwards
                pass

    def test_report_computes_speedup_and_efficiency(self):
        rows = []
        for threads, times in {1: [8.0, 8.2, 7.9], 2: [4.1, 4.0, 4.3], 4: [2.5, 2.4, 2.6]}.items():
            for rep, t in enumerate(times, start=1):
                rows.append({"task": f"threads={threads},repeat={rep}",
                             "params": {"threads": threads, "repeat": rep},
                             "job_id": "1", "node": "n1", "max_rss_mb": 1.0,
                             "elapsed_sec": t})
        summary, axis = reporting.summarize(rows)
        self.assertEqual(axis, "threads")
        by_p = {s["threads"]: s for s in summary}
        self.assertEqual(by_p[1]["median_elapsed_sec"], 8.0)
        self.assertEqual(by_p[2]["speedup"], 1.95)          # 8.0 / 4.1
        self.assertEqual(by_p[4]["efficiency"], 0.8)         # (8.0 / 2.5) / 4


if __name__ == "__main__":
    unittest.main()
