"""SQLite-based registry for persistent workflow execution."""

import json
import sqlite3
from pathlib import Path
from threading import Lock
from typing import Iterator
from datetime import datetime

from zahir.events import WorkflowOutputEvent
from zahir.types import (
    Context,
    DependencyState,
    Job,
    JobRegistry,
    JobState,
    JobInformation,
)


class SQLiteJobRegistry(JobRegistry):
    """Thread-safe SQLite-backed job registry for persistent workflow state."""

    def __init__(self, db_path: str | Path) -> None:
        """Initialize SQLite job registry.

        @param db_path: Path to the SQLite database file
        """
        self.db_path = Path(db_path)
        self._lock = Lock()
        self._init_db()

    def _init_db(self) -> None:
        """Initialize the database schema."""
        with sqlite3.connect(self.db_path) as conn:
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

    def add(self, job: Job) -> str:
        """Register a job with the job registry, returning a job ID.

        @param job: The job to register
        @return: The job ID assigned to the job
        """
        with self._lock:
            job_id = job.job_id
            serialised = json.dumps(job.save())

            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "INSERT INTO jobs (job_id, serialised_job, state) VALUES (?, ?, ?)",
                    (job_id, serialised, JobState.PENDING.value),
                )
                conn.commit()

        return job_id

    def get_state(self, job_id: str) -> JobState:
        """Get the state of a job by ID.

        @param job_id: The ID of the job to get the state of
        @return: The state of the job
        """
        with self._lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT state FROM jobs WHERE job_id = ?", (job_id,)
                )
                row = cursor.fetchone()

                if row is None:
                    raise KeyError(f"Job ID {job_id} not found in registry")

                return JobState(row[0])

    def set_state(self, job_id: str, state: JobState) -> str:
        """Set the state of a job by ID.

        @param job_id: The ID of the job to update
        @param state: The new state of the job
        @return: The ID of the job
        """
        with self._lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "UPDATE jobs SET state = ? WHERE job_id = ?",
                    (state.value, job_id),
                )
                conn.commit()

        return job_id

    def set_output(self, job_id: str, output: dict) -> None:
        """Store the output of a completed job.

        @param job_id: The ID of the job
        @param output: The output dictionary produced by the job
        """
        with self._lock:
            serialised_output = json.dumps(output)

            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO job_outputs (job_id, output)
                    VALUES (?, ?)
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
        """Store timing information for a job.

        @param job_id: The ID of the job
        @param started_at: When the job started execution
        @param completed_at: When the job completed execution
        @param duration_seconds: How long the job took to execute
        """
        with self._lock:
            with sqlite3.connect(self.db_path) as conn:
                updates = []
                params = []

                if started_at is not None:
                    updates.append("started_at = ?")
                    params.append(
                        started_at.isoformat()
                        if hasattr(started_at, "isoformat")
                        else str(started_at)
                    )
                if completed_at is not None:
                    updates.append("completed_at = ?")
                    params.append(
                        completed_at.isoformat()
                        if hasattr(completed_at, "isoformat")
                        else str(completed_at)
                    )
                if duration_seconds is not None:
                    updates.append("duration_seconds = ?")
                    params.append(duration_seconds)

                if updates:
                    params.append(job_id)
                    conn.execute(
                        f"UPDATE jobs SET {', '.join(updates)} WHERE job_id = ?",
                        params,
                    )
                    conn.commit()

    def set_recovery_duration(self, job_id: str, duration_seconds: float) -> None:
        """Store recovery duration for a job.

        @param job_id: The ID of the job
        @param duration_seconds: How long the recovery took
        """
        with self._lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "UPDATE jobs SET recovery_duration_seconds = ? WHERE job_id = ?",
                    (duration_seconds, job_id),
                )
                conn.commit()

    def get_output(self, job_id: str) -> dict | None:
        """Retrieve the output of a completed job.

        @param job_id: The ID of the job
        @return: The output dictionary, or None if no output was set
        """
        with self._lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT output FROM job_outputs WHERE job_id = ?", (job_id,)
                )
                row = cursor.fetchone()

                if row is None:
                    return None

                return json.loads(row[0])

    def outputs(self, workflow_id: str) -> Iterator["WorkflowOutputEvent"]:
        """Get workflow output event containing all job outputs.

        @param workflow_id: The ID of the workflow
        @return: An iterator yielding a WorkflowOutputEvent with all outputs
        """

        output_dict = {}
        with self._lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("SELECT job_id, output FROM job_outputs")
                rows = cursor.fetchall()

        for job_id, serialised_output in rows:
            output_dict[job_id] = json.loads(serialised_output)

        if output_dict:
            yield WorkflowOutputEvent(workflow_id, output_dict)

    def pending(self) -> bool:
        """Check whether any jobs still need to be run.

        @return: True if there are pending jobs, False otherwise
        """
        with self._lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT COUNT(*) FROM jobs WHERE state = ?",
                    (JobState.PENDING.value,),
                )
                count = cursor.fetchone()[0]
                return count > 0

    def running(self, context: Context) -> Iterator[tuple[str, Job]]:
        """Get an iterator of currently running jobs.

        @param context: The context containing scope and registries for deserialization
        @return: An iterator of (job ID, job) tuples for running jobs
        """
        with self._lock:
            running_list = []
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT job_id, serialised_job FROM jobs WHERE state IN (?, ?)",
                    (JobState.RUNNING.value, JobState.RECOVERING.value),
                )

                for row in cursor:
                    job_id, serialised_job = row
                    running_list.append((job_id, serialised_job))

        # Deserialize and yield outside the lock
        for job_id, serialised_job in running_list:
            job_data = json.loads(serialised_job)
            job_type = job_data["type"]
            JobClass = context.scope.get_task_class(job_type)
            job = JobClass.load(context, job_data)
            yield job_id, job

    def runnable(self, context: Context) -> Iterator[tuple[str, Job]]:
        """Yield all runnable jobs from the registry.

        @param context: The context containing scope and registries for deserialization
        @return: An iterator of (job ID, job) tuples for runnable jobs
        """
        with self._lock:
            runnable_list = []

            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT job_id, serialised_job FROM jobs WHERE state = ?",
                    (JobState.PENDING.value,),
                )

                for row in cursor:
                    job_id, serialised_job = row
                    job_data = json.loads(serialised_job)

                    job_type = job_data["type"]
                    JobClass = context.scope.get_task_class(job_type)

                    job = JobClass.load(context, job_data)
                    status = job.ready()

                    if status == DependencyState.SATISFIED:
                        runnable_list.append((job_id, job))
                    elif status == DependencyState.IMPOSSIBLE:
                        # Mark as impossible
                        conn.execute(
                            "UPDATE jobs SET state = ? WHERE job_id = ?",
                            (JobState.IMPOSSIBLE.value, job_id),
                        )

                conn.commit()

        # Yield outside the lock
        for job_id, job in runnable_list:
            yield job_id, job

    def jobs(self, context: Context) -> Iterator["JobInformation"]:
        """Get an iterator of all jobs with their information.

        @param context: The context containing scope and registries for deserialization
        @return: An iterator of JobInformation objects
        """

        with self._lock:
            job_list = []
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT j.job_id, j.serialised_job, j.state, o.output,
                           j.started_at, j.completed_at, j.duration_seconds,
                           j.recovery_duration_seconds
                    FROM jobs j
                    LEFT JOIN job_outputs o ON j.job_id = o.job_id
                """)

                for row in cursor:
                    (
                        job_id,
                        serialised_job,
                        state,
                        output,
                        started_at,
                        completed_at,
                        duration_seconds,
                        recovery_duration_seconds,
                    ) = row
                    job_list.append(
                        (
                            job_id,
                            serialised_job,
                            state,
                            output,
                            started_at,
                            completed_at,
                            duration_seconds,
                            recovery_duration_seconds,
                        )
                    )

        # Deserialize and yield outside the lock
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
            job_data = json.loads(serialised_job)
            job_type = job_data["type"]
            JobClass = context.scope.get_task_class(job_type)
            job = JobClass.load(context, job_data)

            state = JobState(state_str)
            output = json.loads(output_str) if output_str else None

            # Parse datetime strings if present
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
