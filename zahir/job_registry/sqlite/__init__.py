from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timezone
import json
import logging
import multiprocessing
import sqlite3
import traceback
from typing import Any

from zahir.base_types import (
    ACTIVE_JOB_STATES,
    COMPLETED_JOB_STATES,
    Context,
    Job,
    JobInformation,
    JobRegistry,
    JobState,
    JobTimingInformation,
)
from zahir.events import JobCompletedEvent, JobEvent
from zahir.exception import (
    DuplicateJobError,
    MissingJobError,
    exception_from_text_blob,
    exception_to_text_blob,
)
from zahir.job_registry.sqlite.tables import (
    CLAIMED_JOBS_TABLE_SCHEMA,
    JOB_ERRORS_TABLE_SCHEMA,
    JOB_OUTPUTS_TABLE_SCHEMA,
    JOBS_INDEX,
    JOBS_TABLE_SCHEMA,
)
from zahir.job_registry.state_event import create_state_event

log = logging.getLogger(__name__)
# Log level controlled by ZAHIR_LOG_LEVEL environment variable
# Default is WARNING to reduce noise


class SQLiteJobRegistry(JobRegistry):
    conn: sqlite3.Connection

    def __init__(self, db_path: str):
        self._db_path = db_path

    def _create_connection(self) -> sqlite3.Connection:
        """Create and configure a new database connection."""

        log.debug(f"Creating new database connection to {self._db_path}")
        conn = sqlite3.connect(self._db_path, timeout=5.0, isolation_level="IMMEDIATE", check_same_thread=False)

        # WAL-mode for concurrent reads and writes
        conn.execute("PRAGMA journal_mode=WAL;")
        # We do want foreign key constraints
        conn.execute("PRAGMA foreign_keys=ON;")
        # Not too long to wait
        conn.execute("PRAGMA busy_timeout=5000;")

        return conn

    def init(self, worker_id: str) -> None:
        """Establish a database connection, create tables, and clear per-run data."""

        self.conn = self._create_connection()
        log.debug(f"Database connection established to {self._db_path} for {worker_id}")

        with self.conn as conn:
            for schema in [
                JOBS_TABLE_SCHEMA,
                JOB_OUTPUTS_TABLE_SCHEMA,
                JOB_ERRORS_TABLE_SCHEMA,
                CLAIMED_JOBS_TABLE_SCHEMA,
                JOBS_INDEX,
            ]:
                conn.execute(schema)
            conn.commit()

    def on_startup(self) -> None:
        """Delete all job claims. Used at startup to clear any stale claims."""

        log.debug("Clearing all job claims from previous runs")

        with self.conn as conn:
            conn.execute("delete from claimed_jobs;")
            # todo change all job states
            conn.commit()

    def set_claim(self, job_id: str, worker_id: str) -> bool:
        """Set a claim for a job by a worker."""

        log.debug(f"Setting claim for job {job_id} by worker {worker_id}")

        claimed_at = datetime.now(tz=UTC).isoformat()
        with self.conn as conn:
            cursor = conn.execute(
                "insert into claimed_jobs (job_id, claimed_at, claimed_by) values (?, ?, ?);",
                (job_id, claimed_at, worker_id),
            )

            return cursor.lastrowid == 1

    def claim(self, context: Context, worker_id: str) -> Job | None:
        """Claim a job for processing by a worker."""

        log.debug(f"Worker {worker_id} attempting to claim a job")

        claimed_at = datetime.now(tz=UTC).isoformat()
        with self.conn as conn:
            conn.execute("begin immediate;")
            claimed_row = conn.execute(
                """
                insert into claimed_jobs (job_id, claimed_at, claimed_by)
                  select jobs.job_id, ?, ?
                  from jobs
                  left join claimed_jobs
                      on claimed_jobs.job_id = jobs.job_id
                  where jobs.state = ?
                  and claimed_jobs.job_id is null
                  order by random()
                  limit 1
                  returning job_id
                """,
                (claimed_at, worker_id, JobState.READY.value),
            ).fetchone()

            # Bail and end the transaction if no job was claimed
            if claimed_row is None:
                conn.commit()
                return None

            # Same transaction: fetch the job data
            (job_id,) = claimed_row
            job_row = conn.execute("select serialised_job from jobs where job_id = ?", (job_id,)).fetchone()

            conn.commit()

            if job_row is None:
                return None

            # Load the job data
            (serialised_job,) = job_row
            job_data = json.loads(serialised_job)
            job_class = context.scope.get_job_class(job_data["type"])

            return job_class.load(context, job_data)

    def add(self, job: Job, output_queue: multiprocessing.Queue) -> str:
        """Add the job to the database exactly once"""

        log.debug(f"Adding job {job.job_id} to registry")

        job_id = job.job_id
        saved_job = job.save()
        serialised = json.dumps(saved_job)

        # Jobs need their dependencies verified to have passed before they can run;
        # so by default they start as PENDING unless they have no dependencies.
        job_state = JobState.READY.value if job.dependencies.empty() else JobState.PENDING.value

        with self.conn as conn:
            # Check if the job already exists, complain loudly if you try to add it twice
            existing = conn.execute("select 1 from jobs where job_id = ?", (job_id,)).fetchone()
            if existing is not None:
                conn.commit()
                raise DuplicateJobError(f"Job with ID {job_id} already exists in the registry.")

            conn.execute("begin immediate;")
            created_at = datetime.now(tz=UTC).isoformat()
            conn.execute(
                "insert into jobs (job_id, serialised_job, state, created_at) values (?, ?, ?, ?)",
                (job_id, serialised, job_state, created_at),
            )

            conn.commit()

            output_queue.put(JobEvent(job=saved_job))

        return job_id

    def get_state(self, job_id: str) -> JobState:
        """Get the current state of a job. Error if the job is not found."""

        log.debug(f"Getting state for job {job_id}")

        with self.conn as conn:
            row = conn.execute("select state from jobs where job_id = ?", (job_id,)).fetchone()

            if row is None:
                raise MissingJobError(f"Job with ID {job_id} not found in registry.")

            (state_str,) = row
            return JobState(state_str)

    def get_job_timing(self, job_id: str) -> JobTimingInformation:
        """Get timing information for a job."""

        log.debug(f"Getting timing information for job {job_id}")

        with self.conn as conn:
            row = conn.execute(
                """
            select
                started_at,
                recovery_started_at,
                completed_at
            from jobs
            where job_id = ?
            """,
                (job_id,),
            ).fetchone()

            if row is None:
                raise MissingJobError(f"Job with ID {job_id} not found in registry.")

            started_at_str, recovery_started_at_str, completed_at_str = row

            started_at = datetime.fromisoformat(started_at_str) if started_at_str is not None else None
            recovery_started_at = (
                datetime.fromisoformat(recovery_started_at_str) if recovery_started_at_str is not None else None
            )
            completed_at = datetime.fromisoformat(completed_at_str) if completed_at_str is not None else None

            return JobTimingInformation(
                started_at=started_at,
                recovery_started_at=recovery_started_at,
                completed_at=completed_at,
            )

    def is_finished(self, job_id: str) -> bool:
        """Is the job in a terminal state?"""

        log.debug(f"Checking if job {job_id} is finished")

        return self.get_state(job_id) in COMPLETED_JOB_STATES

    def set_state(
        self,
        job_id: str,
        workflow_id: str,
        output_queue: multiprocessing.Queue,
        state: JobState,
        recovery: bool = False,
        error: Exception | None = None,
    ) -> str:
        """Set the state of a job. Optionally add an error if transitioning to a failure state."""

        log.debug(f"Setting state for job {job_id} to {state}")

        with self.conn as conn:
            conn.execute("begin immediate;")
            conn.execute("update jobs set state = ? where job_id = ?", (state.value, job_id))

            if state == JobState.RUNNING:
                # We're starting the job, set ther start-time if not already set
                started_at = datetime.now(tz=UTC).isoformat()
                conn.execute(
                    "update jobs set started_at = ? where job_id = ? and started_at is null", (started_at, job_id)
                )

            if state == JobState.RECOVERING:
                # We're starting recovery, set the recovery start-time if not already set
                recovery_started_at = datetime.now(tz=UTC).isoformat()
                conn.execute(
                    "update jobs set recovery_started_at = ? where job_id = ? and recovery_started_at is null",
                    (recovery_started_at, job_id),
                )

            if state in COMPLETED_JOB_STATES:
                # Job is in a terminal-state; set completed time if not already set
                completed_at = datetime.now(tz=UTC).isoformat()
                conn.execute(
                    "update jobs set completed_at = ? where job_id = ? and completed_at is null", (completed_at, job_id)
                )

            if error is not None:
                error_trace = "".join(traceback.format_exception(type(error), error, error.__traceback__))
                log.warning(f"Recording error for job {job_id}: {error}\nTraceback:\n{error_trace}")

                serialised_error = exception_to_text_blob(error)
                error_text = str(error)
                conn.execute(
                    "insert into job_errors (job_id, error_text, error_blob, recovery) values (?, ?, ?, ?)",
                    (job_id, error_text, serialised_error, int(recovery)),
                )

            conn.commit()

        timing = self.get_job_timing(job_id) if state in COMPLETED_JOB_STATES else None
        event = create_state_event(state, workflow_id, job_id, error=error, timing=timing)
        if event is not None:
            output_queue.put(event)

        return job_id

    def set_output(
        self,
        job_id: str,
        workflow_id: str,
        output_queue: multiprocessing.Queue,
        output: Mapping[str, Any],
        recovery: bool = False,
    ) -> None:
        """Set the output of a job. Mark the job as complete, and emit a completion event."""

        log.debug(f"Setting output for job {job_id} and marking as completed")

        serialised_output = json.dumps(output)
        with self.conn as conn:
            conn.execute("begin immediate;")
            conn.execute(
                """
            insert into job_outputs (job_id, output, recovery)
            values (?, ?, ?)
            on conflict(job_id) do update set output=excluded.output, recovery=excluded.recovery;
            """,
                (job_id, serialised_output, int(recovery)),
            )

            now = datetime.now(tz=UTC).isoformat()
            conn.execute(
                "update jobs set state = ?, completed_at = ? where job_id = ?", (JobState.COMPLETED.value, now, job_id)
            )
            conn.commit()

        # Emit event after transaction completes
        timing = self.get_job_timing(job_id)
        duration = timing.time_since_started() or 0.0
        output_queue.put(
            JobCompletedEvent(
                job_id=job_id,
                workflow_id=workflow_id,
                duration_seconds=duration,
            )
        )

    def get_output(self, job_id: str, recovery: bool = False) -> Mapping[str, Any] | None:
        """Get the output of a completed job.

        @param job_id: The ID of the job to get the output for.
        @param recovery: Whether to get the output for a recovery job.
        @return: The output mapping, or None if no output is found.
        """

        log.debug(f"Getting output for job {job_id} (recovery={recovery})")

        with self.conn as conn:
            row = conn.execute(
                "select output from job_outputs where job_id = ? and recovery = ?", (job_id, int(recovery))
            ).fetchone()

            if row is None:
                return None

            (serialised_output,) = row
            return json.loads(serialised_output)

    def add_error(self, job_id: str, error: Exception, recovery: bool = False) -> None:
        """Add an error for the job."""

        log.debug(f"Adding error for job {job_id}")

        serialised_error = exception_to_text_blob(error)
        error_text = str(error)

        with self.conn as conn:
            conn.execute("begin immediate;")
            conn.execute(
                "insert into job_errors (job_id, error_text, error_blob, recovery) values (?, ?, ?, ?)",
                (job_id, error_text, serialised_error, int(recovery)),
            )
            conn.commit()

    def get_errors(self, job_id: str, recovery: bool = False) -> list[Exception]:
        """Get all errors for a job.

        @param job_id: The ID of the job to get errors for.
        @return: A list of exceptions associated with the job.
        """

        log.debug(f"Getting errors for job {job_id}")

        with self.conn as conn:
            rows = conn.execute(
                "select error_blob from job_errors where job_id = ? and recovery = ?", (job_id, int(recovery))
            ).fetchall()

            errors: list[Exception] = []
            for (error_blob,) in rows:
                error = exception_from_text_blob(error_blob)
                errors.append(error)

            return errors

    def is_active(self) -> bool:
        """Are any jobs active in the registry?

        @return: True if there are any jobs in non-terminal states.
        """

        log.debug("Checking if any jobs are active in the registry")

        with self.conn as conn:
            q_marks = ",".join("?" for _ in ACTIVE_JOB_STATES)
            row = conn.execute(
                f"select 1 from jobs where state in ({q_marks}) limit 1",
                tuple(state.value for state in ACTIVE_JOB_STATES),
            ).fetchone()

        return row is not None

    def jobs(self, context: Context, state: JobState | None = None) -> Iterator[JobInformation]:
        """Retrieve all jobs, optionally filtered by state.

        @param context: The context to use for loading job classes.
        @param state: Optional state to filter jobs by.
        @return: A list of job information
        """

        log.debug(f"Retrieving jobs from registry with state filter: {state}")

        with self.conn as conn:
            job_list = conn.execute("""
          select
            job.job_id,
            job.serialised_job,
            job.state,
            out.output,
            job.started_at,
            job.completed_at
          from jobs as job
          left join job_outputs as out on out.job_id = job.job_id
          """).fetchall()

        for (
            job_id,
            serialised_job,
            state_str,
            serialised_output,
            started_at,
            completed_at,
        ) in job_list:
            job_data = json.loads(serialised_job)
            job_class = context.scope.get_job_class(job_data["type"])
            job = job_class.load(context, job_data)

            job_state = JobState(state_str)
            output = json.loads(serialised_output) if serialised_output is not None else None
            parsed_started_at = datetime.fromisoformat(started_at) if started_at is not None else None
            parsed_completed_at = datetime.fromisoformat(completed_at) if completed_at is not None else None

            if state is not None and job_state != state:
                continue

            yield JobInformation(
                job_id=job_id,
                job=job,
                state=job_state,
                output=output,
                started_at=parsed_started_at,
                completed_at=parsed_completed_at,
            )

    def get_workflow_duration(self) -> float | None:
        """Get the duration of the workflow based on earliest created_at and latest completed_at.

        @return: The duration in seconds, or None if workflow hasn't completed
        """

        log.debug("Computing workflow duration")

        with self.conn as conn:
            row = conn.execute(
                """
            select
                min(created_at) as earliest_created,
                max(completed_at) as latest_completed
            from jobs
            """
            ).fetchone()

            if row is None:
                return None

            earliest_created_str, latest_completed_str = row

            if earliest_created_str is None or latest_completed_str is None:
                return None

            earliest_created = datetime.fromisoformat(earliest_created_str)
            latest_completed = datetime.fromisoformat(latest_completed_str)

            delta = latest_completed - earliest_created
            return delta.total_seconds()

    def close(self) -> None:
        """Close the database connection."""

        log.debug(f"Closing database connection to {self._db_path}")
        if hasattr(self, "conn") and self.conn is not None:
            self.conn.close()
