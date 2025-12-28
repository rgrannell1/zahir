"""SQLite-based registry for persistent workflow execution."""

from collections.abc import Iterator, Mapping
from datetime import datetime
import json
from pathlib import Path
import sqlite3
from typing import cast

from zahir.base_types import (
    Context,
    Job,
    JobInformation,
    JobRegistry,
    JobState,
    SerialisedJob,
)
from zahir.events import WorkflowOutputEvent

JOBS_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id                    TEXT PRIMARY KEY,
    serialised_job            TEXT NOT NULL,
    state                     TEXT NOT NULL,
    created_at                TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at                TIMESTAMP,
    completed_at              TIMESTAMP,
    duration_seconds          REAL,
    recovery_duration_seconds REAL
);
"""

JOB_OUTPUTS_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS job_outputs (
    job_id                    TEXT PRIMARY KEY,
    output                    TEXT NOT NULL,
    FOREIGN KEY (job_id) REFERENCES jobs(job_id)
);
"""

JOBS_INDEX = """
CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state)
"""


class SQLiteJobRegistry(JobRegistry):
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)

        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA busy_timeout=5000;")

        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE;")
            conn.execute(JOBS_TABLE_SCHEMA)
            conn.execute(JOB_OUTPUTS_TABLE_SCHEMA)
            conn.execute(JOBS_INDEX)
            conn.commit()

    def claim(self, context: Context) -> Job | None:
        """Atomically claim a ready job and set its state to CLAIMED."""
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE;")
            row = conn.execute(
                """
                UPDATE jobs
                SET state = ?
                WHERE job_id = (
                    SELECT job_id
                    FROM jobs
                    WHERE state = ?
                    ORDER BY created_at
                    LIMIT 1
                )
                AND state = ?
                RETURNING job_id, serialised_job
                """,
                (
                    JobState.CLAIMED.value,
                    JobState.READY.value,
                    JobState.READY.value,
                ),
            ).fetchone()
            conn.commit()

        if row is None:
            return None

        _, serialised_job = row
        job_data = json.loads(serialised_job)
        JobClass = context.scope.get_job_class(job_data["type"])
        return JobClass.load(context, job_data)

    def add(self, job: Job) -> str:
        job_id = job.job_id
        serialised = json.dumps(job.save())

        # Jobs need their dependencies verified to have passed before they can run;
        # so by default they start as PENDING unless they have no dependencies.
        job_state = (
            JobState.READY.value if job.dependencies.empty() else JobState.PENDING.value
        )

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE;")
            conn.execute(
                "INSERT INTO jobs (job_id, serialised_job, state) VALUES (?, ?, ?)",
                (job_id, serialised, job_state),
            )
            conn.commit()

        return job_id

    def get_state(self, job_id: str) -> JobState:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT state FROM jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()

        if row is None:
            raise KeyError(f"Job ID {job_id} not found in registry")

        return JobState(row[0])

    def set_state(self, job_id: str, state: JobState) -> str:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE;")
            conn.execute(
                "UPDATE jobs SET state = ? WHERE job_id = ?",
                (state.value, job_id),
            )
            conn.commit()
        return job_id

    def set_output(self, job_id: str, output: Mapping) -> None:
        serialised_output = json.dumps(output)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE;")
            conn.execute(
                """
                INSERT INTO job_outputs (job_id, output)
                VALUES (?, ?)
                ON CONFLICT(job_id) DO UPDATE SET output = excluded.output
                """,
                (job_id, serialised_output),
            )
            conn.commit()

    def get_output(self, job_id: str) -> Mapping | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT output FROM job_outputs WHERE job_id = ?",
                (job_id,),
            ).fetchone()

        if row is None:
            return None
        return json.loads(row[0])

    def workflow_outputs(self, workflow_id: str) -> Iterator["WorkflowOutputEvent"]:
        output_dict: dict[str, dict] = {}
        with self._connect() as conn:
            rows = conn.execute("SELECT job_id, output FROM job_outputs").fetchall()

        for job_id, serialised_output in rows:
            output_dict[job_id] = json.loads(serialised_output)

        if output_dict:
            yield WorkflowOutputEvent(output_dict, workflow_id)

    def active(self) -> bool:
        """Return True if any jobs are active (pending, ready, claimed, running, recovering)."""
        active_states = [
            JobState.PENDING.value,
            JobState.READY.value,
            JobState.CLAIMED.value,
            JobState.RUNNING.value,
            JobState.RECOVERING.value,
        ]
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM jobs WHERE state IN ({}) LIMIT 1".format(
                    ",".join("?" for _ in active_states)
                ),
                active_states,
            ).fetchone()
        return row is not None

    def jobs(
        self, context: Context, state: JobState | None = None
    ) -> Iterator["JobInformation"]:
        with self._connect() as conn:
            job_list = conn.execute("""
                SELECT j.job_id, j.serialised_job, j.state, o.output,
                       j.started_at, j.completed_at, j.duration_seconds,
                       j.recovery_duration_seconds
                FROM jobs j
                LEFT JOIN job_outputs o ON j.job_id = o.job_id
            """).fetchall()

        for (
            job_id,
            serialised_job,
            state_str,
            output_str,
            started_at_str,
            completed_at_str,
            duration_seconds,
            recovery_duration_seconds,
        ) in job_list:
            job_data = cast(SerialisedJob, json.loads(serialised_job))
            JobClass = context.scope.get_job_class(job_data["type"])
            job = JobClass.load(context, job_data)

            job_state = JobState(state_str)
            output = json.loads(output_str) if output_str else None
            started_at = (
                datetime.fromisoformat(started_at_str) if started_at_str else None
            )
            completed_at = (
                datetime.fromisoformat(completed_at_str) if completed_at_str else None
            )

            if state is not None and job_state != state:
                continue

            yield JobInformation(
                job_id=job_id,
                job=job,
                state=job_state,
                output=output,
                started_at=started_at,
                completed_at=completed_at,
                duration_seconds=duration_seconds,
                recovery_duration_seconds=recovery_duration_seconds,
            )
