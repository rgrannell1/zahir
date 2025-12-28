"""SQLite-based registry for persistent workflow execution."""

import json
from pathlib import Path
import sqlite3
from threading import Lock

from zahir import events as event_module
from zahir.base_types import (
    EventRegistry,
)
from zahir.events import ZahirEvent


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
        with self._lock, sqlite3.connect(self.db_path) as conn:
            query = (
                "SELECT event_type, event_data, created_at FROM events WHERE 1=1"
            )
            params = []

            if event_type:
                query += " AND event_type = ?"
                params.append(event_type)

            if workflow_id:
                # Use JSON extraction to safely filter by workflow_id
                # This avoids LIKE pattern injection vulnerabilities
                query += " AND json_extract(event_data, '$.workflow_id') = ?"
                params.append(workflow_id)

            query += " ORDER BY created_at"

            cursor = conn.execute(query, params)
            events = []

            for row in cursor:
                event_type_name, event_data, created_at = row
                data = json.loads(event_data)

                # Deserialise event using the appropriate class
                # These are prevended, so no need to use a scope object
                event_class = getattr(event_module, event_type_name, None)
                if event_class and hasattr(event_class, "load"):
                    event = event_class.load(data)
                    events.append(event)

            return events
