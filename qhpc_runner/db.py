from __future__ import annotations

from pathlib import Path
import sqlite3
from datetime import datetime


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS experiments (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    config_hash TEXT NOT NULL UNIQUE,
    config_path TEXT NOT NULL,
    created_at TEXT NOT NULL,
    max_in_flight INTEGER NOT NULL,
    max_attempts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY,
    experiment_id INTEGER NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
    task_key TEXT NOT NULL,
    command TEXT NOT NULL,
    parameters_json TEXT NOT NULL,
    env_json TEXT NOT NULL,
    cpus_per_task INTEGER NOT NULL,
    memory_mb INTEGER NOT NULL,
    walltime_sec INTEGER NOT NULL,
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_failure TEXT,
    UNIQUE(experiment_id, task_key)
);

CREATE TABLE IF NOT EXISTS attempts (
    id INTEGER PRIMARY KEY,
    task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    attempt_no INTEGER NOT NULL,
    submission_token TEXT NOT NULL UNIQUE,
    slurm_job_id TEXT,
    scheduler_state TEXT,
    exit_code TEXT,
    cpus_per_task INTEGER NOT NULL,
    memory_mb INTEGER NOT NULL,
    walltime_sec INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    submitted_at TEXT,
    finished_at TEXT,
    log_path TEXT,
    max_rss_mb REAL,
    elapsed_sec INTEGER,
    UNIQUE(task_id, attempt_no)
);
"""

# Columns added after the first release; older databases get them on open.
MIGRATIONS = [
    ("attempts", "max_rss_mb", "REAL"),
    ("attempts", "elapsed_sec", "INTEGER"),
]


def now_local_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


class Database:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        for table, column, kind in MIGRATIONS:
            cols = {r["name"] for r in self.conn.execute(f"PRAGMA table_info({table})")}
            if column not in cols:
                self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {kind}")
        self.conn.commit()

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def commit(self):
        self.conn.commit()

    def experiment_by_hash(self, config_hash: str):
        return self.conn.execute(
            "SELECT * FROM experiments WHERE config_hash=?", (config_hash,)
        ).fetchone()

    def create_experiment(self, spec):
        cur = self.conn.execute(
            """
            INSERT INTO experiments
            (name, config_hash, config_path, created_at, max_in_flight, max_attempts)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                spec.name,
                spec.config_hash,
                str(spec.path),
                now_local_iso(),
                spec.max_in_flight,
                spec.max_attempts,
            ),
        )
        exp_id = cur.lastrowid
        import json
        for task in spec.tasks:
            self.conn.execute(
                """
                INSERT INTO tasks
                (experiment_id, task_key, command, parameters_json, env_json,
                 cpus_per_task, memory_mb, walltime_sec, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'READY')
                """,
                (
                    exp_id,
                    task.key,
                    task.command,
                    json.dumps(task.parameters, sort_keys=True),
                    json.dumps(task.env, sort_keys=True),
                    task.cpus_per_task,
                    task.memory_mb,
                    task.walltime_sec,
                ),
            )
        self.commit()
        return self.conn.execute("SELECT * FROM experiments WHERE id=?", (exp_id,)).fetchone()

    def tasks_for_experiment(self, exp_id: int):
        return self.conn.execute(
            "SELECT * FROM tasks WHERE experiment_id=? ORDER BY id", (exp_id,)
        ).fetchall()

    def active_tasks(self, exp_id: int):
        return self.conn.execute(
            """
            SELECT * FROM tasks
            WHERE experiment_id=? AND status IN ('SUBMITTING','PENDING','RUNNING')
            ORDER BY id
            """,
            (exp_id,),
        ).fetchall()

    def ready_tasks(self, exp_id: int):
        return self.conn.execute(
            """
            SELECT * FROM tasks
            WHERE experiment_id=? AND status IN ('READY','RETRY_READY')
            ORDER BY id
            """,
            (exp_id,),
        ).fetchall()

    def latest_attempt(self, task_id: int):
        return self.conn.execute(
            "SELECT * FROM attempts WHERE task_id=? ORDER BY attempt_no DESC LIMIT 1",
            (task_id,),
        ).fetchone()

    def active_attempts(self, exp_id: int):
        return self.conn.execute(
            """
            SELECT a.*, t.task_key, t.status AS task_status
            FROM attempts a
            JOIN tasks t ON t.id=a.task_id
            WHERE t.experiment_id=?
              AND t.status IN ('SUBMITTING','PENDING','RUNNING')
              -- only the task's current attempt: an earlier, already-failed
              -- attempt must not be re-classified on every sync
              AND a.attempt_no = t.attempt_count
            ORDER BY a.id
            """,
            (exp_id,),
        ).fetchall()

    def create_attempt(self, task, token: str, resources: dict, log_path: str):
        attempt_no = int(task["attempt_count"]) + 1
        self.conn.execute(
            """
            INSERT INTO attempts
            (task_id, attempt_no, submission_token, cpus_per_task, memory_mb,
             walltime_sec, created_at, log_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task["id"],
                attempt_no,
                token,
                resources["cpus_per_task"],
                resources["memory_mb"],
                resources["walltime_sec"],
                now_local_iso(),
                log_path,
            ),
        )
        self.conn.execute(
            """
            UPDATE tasks
            SET status='SUBMITTING', attempt_count=?
            WHERE id=?
            """,
            (attempt_no, task["id"]),
        )
        self.commit()
        return self.latest_attempt(task["id"])

    def record_submission(self, attempt_id: int, job_id: str):
        self.conn.execute(
            """
            UPDATE attempts
            SET slurm_job_id=?, scheduler_state='PENDING', submitted_at=?
            WHERE id=?
            """,
            (job_id, now_local_iso(), attempt_id),
        )
        self.conn.execute(
            """
            UPDATE tasks SET status='PENDING'
            WHERE id=(SELECT task_id FROM attempts WHERE id=?)
            """,
            (attempt_id,),
        )
        self.commit()

    def update_from_scheduler(self, attempt_id: int, record):
        self.conn.execute(
            """
            UPDATE attempts
            SET scheduler_state=?,
                exit_code=COALESCE(?, exit_code),
                slurm_job_id=COALESCE(slurm_job_id, ?),
                submitted_at=COALESCE(submitted_at, ?),
                max_rss_mb=COALESCE(?, max_rss_mb),
                elapsed_sec=COALESCE(?, elapsed_sec)
            WHERE id=?
            """,
            (record.state, record.exit_code, record.job_id, now_local_iso(),
             record.max_rss_mb, record.elapsed_sec, attempt_id),
        )
        self.commit()

    def mark_attempt(self, attempt_id: int, scheduler_state: str):
        """Close an attempt that never reached (or was detached from) Slurm."""
        self.conn.execute(
            "UPDATE attempts SET scheduler_state=?, finished_at=? WHERE id=?",
            (scheduler_state, now_local_iso(), attempt_id),
        )
        self.commit()

    def task_by_key(self, exp_id: int, task_key: str):
        return self.conn.execute(
            "SELECT * FROM tasks WHERE experiment_id=? AND task_key=?", (exp_id, task_key)
        ).fetchone()

    def attempts_for_task(self, task_id: int):
        return self.conn.execute(
            "SELECT * FROM attempts WHERE task_id=? ORDER BY attempt_no", (task_id,)
        ).fetchall()

    def finish_attempt(self, attempt_id: int, state: str, exit_code: str | None):
        self.conn.execute(
            """
            UPDATE attempts
            SET scheduler_state=?, exit_code=?, finished_at=?
            WHERE id=?
            """,
            (state, exit_code, now_local_iso(), attempt_id),
        )
        self.commit()

    def set_task_status(self, task_id: int, status: str, failure: str | None = None):
        self.conn.execute(
            "UPDATE tasks SET status=?, last_failure=? WHERE id=?",
            (status, failure, task_id),
        )
        self.commit()

    def attempt_by_id(self, attempt_id: int):
        return self.conn.execute("SELECT * FROM attempts WHERE id=?", (attempt_id,)).fetchone()

    def task_by_id(self, task_id: int):
        return self.conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()

    def counts(self, exp_id: int):
        rows = self.conn.execute(
            "SELECT status, COUNT(*) AS n FROM tasks WHERE experiment_id=? GROUP BY status",
            (exp_id,),
        ).fetchall()
        return {r["status"]: r["n"] for r in rows}
