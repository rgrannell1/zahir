"""SQLite-based registry for persistent workflow execution."""

import json
import sqlite3
from pathlib import Path
from threading import Lock
from typing import Iterator

from zahir import events as event_module
from zahir.events import ZahirEvent
from zahir.types import (
    Context,
    DependencyState,
    EventRegistry,
    Job,
    JobRegistry,
    JobState
)


class SQLiteJobRegistry(JobRegistry):
    """Thread-safe SQLite-backed job registry for persistent workflow state."""

    def __init__(self, db_path: str | Path, context: Context) -> None:
        """Initialize SQLite job registry.

        @param db_path: Path to the SQLite database file
        @param context: Context containing scope and registries for deserialization
        """
        self.db_path = Path(db_path)
        self.context = context
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
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

    def runnable(self) -> Iterator[tuple[str, Job]]:
        """Yield all runnable jobs from the registry.

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
                    JobClass = self.context.scope.get_task_class(job_type)

                    job = JobClass.load(self.context, job_data)
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


class SQLiteEventRegistry(EventRegistry):
    """SQLite-backed event registry for persistent event storage."""

    def __init__(self, db_path: str | Path) -> None:
        """Initialize SQLite event registry.

        @param db_path: Path to the SQLite database file
        """
        self.db_path = Path(db_path)
        self._lock = Lock()
        self._init_db()

    def _init_db(self) -> None:
        """Initialize the database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    event_data TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_events_type
                ON events(event_type)
            """)
            conn.commit()

    def register(self, event: ZahirEvent) -> None:
        """Register an event in the event registry.

        @param event: The event to register
        """
        with self._lock:
            event_type = type(event).__name__
            # Use the event's save method for proper serialization
            event_data = json.dumps(event.save())

            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "INSERT INTO events (event_type, event_data) VALUES (?, ?)",
                    (event_type, event_data),
                )
                conn.commit()

    def get_events(
        self, event_type: str | None = None, workflow_id: str | None = None
    ) -> list[ZahirEvent]:
        """Retrieve events from the registry.

        @param event_type: Optional filter by event type
        @param workflow_id: Optional filter by workflow ID
        @return: List of deserialised event objects
        """
        with self._lock:
            with sqlite3.connect(self.db_path) as conn:
                query = "SELECT event_type, event_data, created_at FROM events WHERE 1=1"
                params = []

                if event_type:
                    query += " AND event_type = ?"
                    params.append(event_type)

                if workflow_id:
                    query += " AND event_data LIKE ?"
                    params.append(f'%"workflow_id": "{workflow_id}"%')

                query += " ORDER BY created_at"

                cursor = conn.execute(query, params)
                events = []

                for row in cursor:
                    event_type_name, event_data, created_at = row
                    data = json.loads(event_data)

                    # Deserialise event using the appropriate class
                    # These are prevended, so no need to use a scope object
                    event_class = getattr(event_module, event_type_name, None)
                    if event_class and hasattr(event_class, 'load'):
                        event = event_class.load(data)
                        events.append(event)

                return events
