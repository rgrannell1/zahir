import json
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime
import multiprocessing
import sqlite3
import traceback
from typing import Any, cast

from zahir.base_types import (
    ACTIVE_JOB_STATES,
    COMPLETED_JOB_STATES,
    Context,
    JobInformation,
    JobInstance,
    JobRegistry,
    JobState,
    JobTimingInformation,
    SerialisedJobInstance,
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
    EVENTS_TABLE_SCHEMA,
    JOB_ERRORS_TABLE_SCHEMA,
    JOB_OUTPUTS_TABLE_SCHEMA,
    JOBS_IDEMPOTENCY_INDEX,
    JOBS_INDEX,
    JOBS_PRIORITY_INDEX,
    JOBS_TABLE_SCHEMA,
)
from zahir.job_registry.state_event import create_state_event
from zahir.serialise import serialise_event
from zahir.utils.hash import compute_idempotence_key
from zahir.utils.logging_config import get_logger

log = get_logger(__name__)
# Log level controlled by ZAHIR_LOG_LEVEL environment variable
# Default is WARNING to reduce noise




class SQLiteJobRegistry(JobRegistry):
    conn: sqlite3.Connection

    def __init__(self, db_path: str):
        self._db_path = db_path

    def _create_connection(self) -> sqlite3.Connection:
        """Create and configure a new database connection."""

        log.debug(f"Creating new database connection to {self._db_path}")
        # Use isolation_level=None for manual transaction control to avoid double-locking
        conn = sqlite3.connect(self._db_path, timeout=5, isolation_level=None, check_same_thread=False)

        # WAL-mode for concurrent reads and writes
        conn.execute("PRAGMA journal_mode=WAL;")
        # We do want foreign key constraints
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA busy_timeout=5000;")
        conn.execute("PRAGMA synchronous=NORMAL;")

        return conn

    def init(self, worker_id: str) -> None:
        """Establish a database connection, create tables, and clear per-run data."""

        self.conn = self._create_connection()
        log.debug(f"Database connection established to {self._db_path} for {worker_id}")

        with self.conn as conn:
            conn.execute("begin;")
            for schema in [
                JOBS_TABLE_SCHEMA,
                JOB_OUTPUTS_TABLE_SCHEMA,
                JOB_ERRORS_TABLE_SCHEMA,
                CLAIMED_JOBS_TABLE_SCHEMA,
                EVENTS_TABLE_SCHEMA,
                JOBS_INDEX,
                JOBS_PRIORITY_INDEX,
                JOBS_IDEMPOTENCY_INDEX,
            ]:
                conn.execute(schema)
            conn.commit()

    def store_event(self, context, event: Any) -> None:
        """Store an event in the events table."""

        event_type = type(event).__name__
        created_at = datetime.now(tz=UTC).isoformat()

        if not hasattr(event, "save"):
            return

        event_data = event.save(context)
        event_blob = json.dumps(event_data)

        workflow_id = getattr(event, "workflow_id", "")
        job_id = getattr(event, "job_id", "")

        with self.conn as conn:
            conn.execute(
                """
                insert into events (workflow_id, job_id, event_type, event_blob, created_at)
                values (?, ?, ?, ?, ?)
                """,
                (workflow_id, job_id, event_type, event_blob, created_at),
            )
            conn.commit()

    def on_startup(self) -> None:
        """Delete all job claims. Used at startup to clear any stale claims."""

        log.debug("Clearing all job claims from previous runs")

        with self.conn as conn:
            conn.execute("begin immediate;")
            conn.execute("delete from claimed_jobs;")

            # Reset all active (non-terminal) jobs to PENDING
            # This includes READY, RUNNING, PAUSED, RECOVERING, PENDING, BLOCKED
            q_marks = ",".join("?" for _ in ACTIVE_JOB_STATES)
            conn.execute(
                f"update jobs set state = ? where state in ({q_marks})",  # noqa: S608
                (JobState.PENDING.value, *[state.value for state in ACTIVE_JOB_STATES]),
            )

            conn.commit()

    def set_claim(self, job_id: str, worker_id: str) -> bool:
        """Set a claim for a job by a worker. Used by the overseer when dispatching jobs."""

        log.debug(f"Setting claim for job {job_id} by worker {worker_id}")

        claimed_at = datetime.now(tz=UTC).isoformat()
        with self.conn as conn:
            conn.execute("begin immediate;")
            cursor = conn.execute(
                "insert into claimed_jobs (job_id, claimed_at, claimed_by) values (?, ?, ?);",
                (job_id, claimed_at, worker_id),
            )
            conn.commit()

            return cursor.lastrowid == 1

    def add(self, context: Context, job: JobInstance, output_queue: multiprocessing.Queue) -> str:
        """Add the job to the database exactly once"""

        job_id = job.args.job_id
        log.debug(f"Adding job {job_id} to registry")

        saved_job = job.save(context)
        serialised = json.dumps(saved_job)

        # Jobs need their dependencies verified to have passed before they can run;
        # so by default they start as PENDING unless they have no dependencies.
        job_state = JobState.READY.value if job.args.dependencies.empty() else JobState.PENDING.value

        with self.conn as conn:
            conn.execute("begin immediate;")

            # Check if the job already exists, complain loudly if you try to add it twice
            existing = conn.execute("select 1 from jobs where job_id = ?", (job_id,)).fetchone()
            if existing is not None:
                conn.rollback()
                raise DuplicateJobError(f"Job with ID {job_id} already exists in the registry.")

            # If once flag is set, check for existing job with same idempotency hash
            idempotency_hash = None
            if job.args.once:
                # Use custom once_by function if provided, otherwise use default
                idempotency_hash = compute_idempotence_key(
                    job_type=saved_job["type"],
                    args=saved_job["args"],
                    dependencies=saved_job["dependencies"],
                    once_by=getattr(job.args, "once_by", None),
                )
                existing_hash = conn.execute(
                    "select job_id from jobs where idempotency_hash = ?", (idempotency_hash,)
                ).fetchone()

                if existing_hash is not None:
                    existing_job_id = existing_hash[0]
                    log.debug(f"Job with idempotency_hash {idempotency_hash} already exists as {existing_job_id}, skipping insert")
                    conn.rollback()
                    return existing_job_id

            created_at = datetime.now(tz=UTC).isoformat()
            priority = job.args.priority
            conn.execute(
                "insert into jobs (job_id, serialised_job, state, priority, created_at, idempotency_hash) values (?, ?, ?, ?, ?, ?)",
                (job_id, serialised, job_state, priority, created_at, idempotency_hash),
            )

            conn.commit()

            output_queue.put(serialise_event(context, JobEvent(job=cast(SerialisedJobInstance, saved_job))))

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
        context: Context,
        job_id: str,
        job_type: str,
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
                # Release claim when job reaches terminal state
                conn.execute("delete from claimed_jobs where job_id = ?", (job_id,))

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
        event = create_state_event(state, workflow_id, job_id, job_type, error=error, timing=timing)
        if event is not None:
            output_queue.put(serialise_event(context, event))

        return job_id

    def set_output(
        self,
        context: Context,
        job_id: str,
        job_type: str,
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
        output_queue.put(serialise_event(
            context,
            JobCompletedEvent(
                job_id=job_id,
                job_type=job_type,
                workflow_id=workflow_id,
                duration_seconds=duration,
            ))
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
                f"select 1 from jobs where state in ({q_marks}) limit 1",  # noqa: S608
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
          order by job.priority desc, job.created_at asc
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
            spec = context.scope.get_job_spec(job_data["type"])

            from zahir.base_types import JobInstance

            job = JobInstance.load(context, job_data)

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
