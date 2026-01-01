"""SQLite-based registry for persistent workflow execution."""

from collections.abc import Iterator, Mapping
from datetime import datetime
import json
from pathlib import Path
import sqlite3
import time
from typing import cast

from zahir.base_types import (
    COMPLETED_JOB_STATES,
    Context,
    Job,
    JobInformation,
    JobRegistry,
    JobState,
    SerialisedJob,
)
from zahir.events import (
    JobCompletedEvent,
    JobEvent,
    JobIrrecoverableEvent,
    JobPausedEvent,
    JobPrecheckFailedEvent,
    JobRecoveryCompletedEvent,
    JobRecoveryStartedEvent,
    JobRecoveryTimeoutEvent,
    JobStartedEvent,
    JobTimeoutEvent,
    WorkflowOutputEvent,
)
from zahir.exception import exception_from_text_blob, exception_to_text_blob

JOBS_TABLE_SCHEMA = """
create table if not exists jobs (
    job_id                    text primary key,
    serialised_job            text not null,
    state                     text not null,
    created_at                timestamp default current_timestamp,
    started_at                timestamp,
    completed_at              timestamp,
    duration_seconds          real,
    recovery_duration_seconds real
);
"""

JOB_OUTPUTS_TABLE_SCHEMA = """
create table if not exists job_outputs (
    job_id                    text primary key,
    output                    text not null,
    foreign key (job_id) references jobs(job_id)
);
"""

JOB_ERRORS_TABLE_SCHEMA = """
create table if not exists job_errors (
    job_id                    text,
    error                     text not null,
    foreign key (job_id) references jobs(job_id)
);
"""

CLAIMED_JOBS_TABLE_SCHEMA = """
create table if not exists claimed_jobs (
    job_id                    text primary key,
    claimed_at                timestamp default current_timestamp,
    claimed_by                text not null,
    foreign key (job_id) references jobs(job_id)
);
"""

JOBS_INDEX = """
CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state)
"""




class SQLiteJobRegistry(JobRegistry):
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self._init_db()
        self.clear_claims()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.db_path,
            timeout=30.0,
            isolation_level="DEFERRED",  # Use deferred transactions
            check_same_thread=False
        )

        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA busy_timeout=5000;")  # 30 seconds

        return conn

    def clear_claims(self) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM claimed_jobs;")
            conn.commit()

    def _init_db(self) -> None:
        with self._connect() as conn:
            # Set WAL mode once during initialization
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")

            for schema in [
                JOBS_TABLE_SCHEMA,
                JOB_OUTPUTS_TABLE_SCHEMA,
                JOB_ERRORS_TABLE_SCHEMA,
                CLAIMED_JOBS_TABLE_SCHEMA,
                JOBS_INDEX,
            ]:
                conn.execute(schema)

            conn.commit()

    def set_claim(self, job_id: str, pid: int) -> bool:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            insert_cursor = conn.execute(
                """
                INSERT OR IGNORE INTO claimed_jobs (job_id, claimed_by)
                VALUES (?, ?)
                """,
                (job_id, str(pid)),
            )
            conn.commit()

            return insert_cursor.rowcount == 1

    def claim(self, context: Context, pid: int) -> Job | None:
        max_retries = 10
        base_delay = 0.1  # 100ms

        for attempt in range(max_retries):
            try:
                with self._connect() as conn:
                    conn.execute("BEGIN IMMEDIATE")

                    # Atomically: choose the oldest READY unclaimed job (FIFO is fair)
                    # insert a claim row, return job_id.
                    claimed_row = conn.execute(
                        """
                        INSERT INTO claimed_jobs (job_id, claimed_by)
                        SELECT jobs.job_id, ?
                        FROM jobs
                        LEFT JOIN claimed_jobs
                            ON claimed_jobs.job_id = jobs.job_id
                        WHERE jobs.state = ?
                        AND claimed_jobs.job_id IS NULL
                        ORDER BY jobs.created_at
                        LIMIT 1
                        RETURNING job_id
                        """,
                        (str(pid), JobState.READY.value),
                    ).fetchone()

                    if claimed_row is None:
                        conn.commit()
                        return None

                    (job_id,) = claimed_row

                    # Still within the same transaction: fetch the serialised job we just claimed.
                    job_row = conn.execute(
                        "SELECT serialised_job FROM jobs WHERE job_id = ?",
                        (job_id,),
                    ).fetchone()

                    conn.commit()

                if job_row is None:
                    # Extremely unlikely unless the jobs row was deleted outside FK enforcement.
                    return None

                (serialised_job,) = job_row
                job_data = json.loads(serialised_job)
                job_class = context.scope.get_job_class(job_data["type"])
                return job_class.load(context, job_data)

            except (sqlite3.OperationalError, sqlite3.DatabaseError) as exc:
                if attempt < max_retries - 1:
                    # Exponential backoff with jitter
                    delay = base_delay * (2 ** attempt) * (0.5 + 0.5 * time.time() % 1)
                    time.sleep(delay)
                    continue
                else:
                    # Final attempt failed, re-raise
                    raise

    def add(self, job: Job, output_queue) -> str:
        job_id = job.job_id
        saved_job = job.save()
        serialised = json.dumps(saved_job)

        # Jobs need their dependencies verified to have passed before they can run;
        # so by default they start as PENDING unless they have no dependencies.
        job_state = JobState.READY.value if job.dependencies.empty() else JobState.PENDING.value

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT INTO jobs (job_id, serialised_job, state) VALUES (?, ?, ?)",
                (job_id, serialised, job_state),
            )
            conn.commit()
            output_queue.put(JobEvent(job=saved_job))

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

    def is_finished(self, job_id: str) -> bool:
        return self.get_state(job_id) in COMPLETED_JOB_STATES

    def set_state(self, job_id: str, workflow_id: str, output_queue, state: JobState, **kwargs) -> str:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "UPDATE jobs SET state = ? WHERE job_id = ?",
                (state.value, job_id),
            )
            conn.commit()

        if state == JobState.PRECHECK_FAILED:
            error = kwargs["error"]

            self.set_error(job_id, error)
            output_queue.put(
                JobPrecheckFailedEvent(workflow_id=workflow_id, job_id=job_id, error=exception_to_text_blob(error))
            )
        elif state == JobState.RUNNING:
            output_queue.put(JobStartedEvent(workflow_id=workflow_id, job_id=job_id))
        elif state == JobState.PAUSED:
            output_queue.put(JobPausedEvent(workflow_id=workflow_id, job_id=job_id))
        elif state == JobState.COMPLETED:
            output_queue.put(
                JobCompletedEvent(
                    job_id=job_id,
                    workflow_id=workflow_id,
                    duration_seconds=0.1,
                )
            )
        elif state == JobState.TIMED_OUT:
            error = kwargs["error"]
            self.set_error(job_id, error)

            output_queue.put(
                JobTimeoutEvent(
                    job_id=job_id,
                    workflow_id=workflow_id,
                    duration_seconds=0.1,
                )
            )
        elif state == JobState.RECOVERING:
            output_queue.put(
                JobRecoveryStartedEvent(
                    job_id=job_id,
                    workflow_id=workflow_id,
                )
            )
        elif state == JobState.RECOVERED:
            output_queue.put(
                JobRecoveryCompletedEvent(
                    job_id=job_id,
                    workflow_id=workflow_id,
                    duration_seconds=0.1,
                )
            )
        elif state == JobState.RECOVERY_TIMED_OUT:
            error = kwargs["error"]
            self.set_error(job_id, error)

            output_queue.put(
                JobRecoveryTimeoutEvent(
                    job_id=job_id,
                    workflow_id=workflow_id,
                    duration_seconds=0.1,
                )
            )
        elif state == JobState.IRRECOVERABLE:
            error = kwargs["error"]
            self.set_error(job_id, error)

            output_queue.put(JobIrrecoverableEvent(job_id=job_id, workflow_id=workflow_id, error=error))
        elif state == JobState.IMPOSSIBLE:
            error = kwargs["error"]
            self.set_error(job_id, error)


        return job_id


    def set_output(self, job_id: str, workflow_id: str, output_queue, output: Mapping) -> None:
        serialised_output = json.dumps(output)

        # Set output and state in a single transaction
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT INTO job_outputs (job_id, output)
                VALUES (?, ?)
                ON CONFLICT(job_id) DO UPDATE SET output = excluded.output
                """,
                (job_id, serialised_output),
            )

            conn.execute(
                "UPDATE jobs SET state = ? WHERE job_id = ?",
                (JobState.COMPLETED.value, job_id),
            )

            conn.commit()

        # Emit event after transaction completes
        output_queue.put(
            JobCompletedEvent(
                job_id=job_id,
                workflow_id=workflow_id,
                duration_seconds=0.1,
            )
        )

    def get_output(self, job_id: str) -> Mapping | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT output FROM job_outputs WHERE job_id = ?",
                (job_id,),
            ).fetchone()

        if row is None:
            return None
        return json.loads(row[0])

    def set_error(self, job_id: str, error: Exception) -> None:
        serialised_error = exception_to_text_blob(error)

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT INTO job_errors (job_id, error)
                VALUES (?, ?)
                """,
                (job_id, serialised_error),
            )
            conn.commit()

    def get_errors(self, job_id: str) -> list[BaseException]:
        """Get all errors associated with a job."""

        with self._connect() as conn:
            rows = conn.execute(
                "SELECT error FROM job_errors WHERE job_id = ?",
                (job_id,),
            ).fetchall()

        errors: list[BaseException] = []

        for (serialised_error,) in rows:
            error = exception_from_text_blob(serialised_error)
            errors.append(error)

        return errors

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
            JobState.RUNNING.value,
            JobState.RECOVERING.value,
            JobState.PAUSED.value,
        ]
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM jobs WHERE state IN ({}) LIMIT 1".format(",".join("?" for _ in active_states)),
                active_states,
            ).fetchone()

        return row is not None

    def jobs(self, context: Context, state: JobState | None = None) -> Iterator["JobInformation"]:
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
            job_class = context.scope.get_job_class(job_data["type"])
            job = job_class.load(context, job_data)

            job_state = JobState(state_str)
            output = json.loads(output_str) if output_str else None
            started_at = datetime.fromisoformat(started_at_str) if started_at_str else None
            completed_at = datetime.fromisoformat(completed_at_str) if completed_at_str else None

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
