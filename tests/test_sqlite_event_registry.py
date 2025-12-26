import os
import tempfile
from zahir.event_registry.sqlite import SQLiteEventRegistry
from zahir.events import WorkflowCompleteEvent

def test_sqlite_event_registry_register_and_query():
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        db_path = tmp.name
    try:
        registry = SQLiteEventRegistry(db_path)
        event = WorkflowCompleteEvent(workflow_id="wf1", duration_seconds=1.23)
        registry.register(event)
        # Directly query the DB to check event was written
        import sqlite3, json
        with sqlite3.connect(db_path) as conn:
            cur = conn.execute("SELECT event_type, event_data FROM events")
            rows = cur.fetchall()
            assert any(r[0] == "WorkflowCompleteEvent" and json.loads(r[1])["workflow_id"] == "wf1" for r in rows)
    finally:
        os.remove(db_path)
