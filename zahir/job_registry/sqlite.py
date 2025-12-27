"""SQLite-based registry for persistent workflow execution."""

import json
import sqlite3
from pathlib import Path
from typing import Iterator, Mapping, cast
from datetime import datetime

from zahir.events import WorkflowOutputEvent
from zahir.base_types import (
    Context,
    Job,
    JobRegistry,
    JobState,
    JobInformation,
    SerialisedJob,
)


class SQLiteJobRegistry(JobRegistry):
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        # Consistent behavior per connection
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute(
            "PRAGMA synchronous=NORMAL;"
        )  # use FULL if you want max durability
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA busy_timeout=5000;")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE;")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    serialised_job TEXT NOT NULL,
                    state TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    duration_seconds REAL,
                    recovery_duration_seconds REAL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS job_outputs (
                    job_id TEXT PRIMARY KEY,
                    output TEXT NOT NULL,
                    FOREIGN KEY (job_id) REFERENCES jobs(job_id)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_jobs_state
                ON jobs(state)
            """)
            conn.commit()

    def claim(self, context: Context) -> Job | None:
        """Atomically claim a pending job and set its state to CLAIMED."""
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
                    JobState.PENDING.value,
                    JobState.PENDING.value,
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

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE;")
            conn.execute(
                "INSERT INTO jobs (job_id, serialised_job, state) VALUES (?, ?, ?)",
                (job_id, serialised, JobState.PENDING.value),
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

    def set_timing(
        self,
        job_id: str,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
        duration_seconds: float | None = None,
    ) -> None:
        # TODO deprecate
        updates: list[str] = []
        params: list[object] = []

        if started_at is not None:
            updates.append("started_at = ?")
            params.append(started_at.isoformat())
        if completed_at is not None:
            updates.append("completed_at = ?")
            params.append(completed_at.isoformat())
        if duration_seconds is not None:
            updates.append("duration_seconds = ?")
            params.append(duration_seconds)

        if not updates:
            return

        params.append(job_id)

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE;")
            conn.execute(
                f"UPDATE jobs SET {', '.join(updates)} WHERE job_id = ?",
                params,
            )
            conn.commit()

    def set_recovery_duration(self, job_id: str, duration_seconds: float) -> None:
        # TODO deprecate

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE;")
            conn.execute(
                "UPDATE jobs SET recovery_duration_seconds = ? WHERE job_id = ?",
                (duration_seconds, job_id),
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

    def pending(self) -> bool:
        # TODO deprecate
        with self._connect() as conn:
            (count,) = conn.execute(
                "SELECT COUNT(*) FROM jobs WHERE state = ?",
                (JobState.PENDING.value,),
            ).fetchone()
        return count > 0

    def jobs(self, context: Context) -> Iterator["JobInformation"]:
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

            state = JobState(state_str)
            output = json.loads(output_str) if output_str else None
            started_at = (
                datetime.fromisoformat(started_at_str) if started_at_str else None
            )
            completed_at = (
                datetime.fromisoformat(completed_at_str) if completed_at_str else None
            )

            yield JobInformation(
                job_id=job_id,
                job=job,
                state=state,
                output=output,
                started_at=started_at,
                completed_at=completed_at,
                duration_seconds=duration_seconds,
                recovery_duration_seconds=recovery_duration_seconds,
            )
