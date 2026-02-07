"""Tests for SqliteDependency: status-column mode, cardinality mode, save, load."""

import pathlib
import tempfile

import pytest

from zahir.base_types import DependencyState
from zahir.context.memory import MemoryContext
from zahir.dependencies.sqlite import SqliteDependency
from zahir.job_registry import SQLiteJobRegistry
from zahir.scope import LocalScope


def _tmp_db():
    """Create a temporary SQLite database path; caller must unlink when done."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        return tmp.name


# ---- Status-column mode: one row × one column named "status" ----


def test_sqlite_dependency_status_column_satisfied():
    """Status column mode: status='satisfied' -> SATISFIED."""
    db_path = _tmp_db()
    try:
        import sqlite3

        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE gate (status TEXT)")
            conn.execute("INSERT INTO gate (status) VALUES (?)", ("satisfied",))
        dep = SqliteDependency(db_path, "SELECT status FROM gate")
        assert dep.satisfied() == DependencyState.SATISFIED
    finally:
        pathlib.Path(db_path).unlink(missing_ok=True)


def test_sqlite_dependency_status_column_unsatisfied():
    """Status column mode: status='unsatisfied' -> UNSATISFIED."""
    db_path = _tmp_db()
    try:
        import sqlite3

        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE gate (status TEXT)")
            conn.execute("INSERT INTO gate (status) VALUES (?)", ("unsatisfied",))
        dep = SqliteDependency(db_path, "SELECT status FROM gate")
        assert dep.satisfied() == DependencyState.UNSATISFIED
    finally:
        pathlib.Path(db_path).unlink(missing_ok=True)


def test_sqlite_dependency_status_column_impossible():
    """Status column mode: status='impossible' -> IMPOSSIBLE."""
    db_path = _tmp_db()
    try:
        import sqlite3

        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE gate (status TEXT)")
            conn.execute("INSERT INTO gate (status) VALUES (?)", ("impossible",))
        dep = SqliteDependency(db_path, "SELECT status FROM gate")
        assert dep.satisfied() == DependencyState.IMPOSSIBLE
    finally:
        pathlib.Path(db_path).unlink(missing_ok=True)


def test_sqlite_dependency_status_column_case_insensitive():
    """Status column mode: value is lowercased and stripped."""
    db_path = _tmp_db()
    try:
        import sqlite3

        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE gate (status TEXT)")
            conn.execute("INSERT INTO gate (status) VALUES (?)", ("  SATISFIED  ",))
        dep = SqliteDependency(db_path, "SELECT status FROM gate")
        assert dep.satisfied() == DependencyState.SATISFIED
    finally:
        pathlib.Path(db_path).unlink(missing_ok=True)


def test_sqlite_dependency_status_column_invalid_raises():
    """Status column mode: invalid status value raises ValueError."""
    db_path = _tmp_db()
    try:
        import sqlite3

        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE gate (status TEXT)")
            conn.execute("INSERT INTO gate (status) VALUES (?)", ("invalid",))
        dep = SqliteDependency(db_path, "SELECT status FROM gate")
        with pytest.raises(ValueError, match="Invalid status"):
            dep.satisfied()
    finally:
        pathlib.Path(db_path).unlink(missing_ok=True)


# ---- Cardinality mode: use row count (no single 'status' column) ----


def test_sqlite_dependency_cardinality_zero_rows_unsatisfied():
    """Cardinality mode: zero rows -> UNSATISFIED."""
    db_path = _tmp_db()
    try:
        import sqlite3

        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE ready (id INTEGER)")
        dep = SqliteDependency(db_path, "SELECT id FROM ready")
        assert dep.satisfied() == DependencyState.UNSATISFIED
    finally:
        pathlib.Path(db_path).unlink(missing_ok=True)


def test_sqlite_dependency_cardinality_one_row_satisfied():
    """Cardinality mode: one row (but not single 'status' column) -> SATISFIED."""
    db_path = _tmp_db()
    try:
        import sqlite3

        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE ready (id INTEGER)")
            conn.execute("INSERT INTO ready (id) VALUES (1)")
        dep = SqliteDependency(db_path, "SELECT id FROM ready")
        assert dep.satisfied() == DependencyState.SATISFIED
    finally:
        pathlib.Path(db_path).unlink(missing_ok=True)


def test_sqlite_dependency_cardinality_multiple_rows_satisfied():
    """Cardinality mode: multiple rows -> SATISFIED (fetchone returns first)."""
    db_path = _tmp_db()
    try:
        import sqlite3

        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE ready (id INTEGER)")
            conn.execute("INSERT INTO ready (id) VALUES (1), (2)")
        dep = SqliteDependency(db_path, "SELECT id FROM ready")
        assert dep.satisfied() == DependencyState.SATISFIED
    finally:
        pathlib.Path(db_path).unlink(missing_ok=True)


def test_sqlite_dependency_cardinality_two_columns_not_status():
    """Cardinality mode: one row with two columns (no 'status' column) -> SATISFIED."""
    db_path = _tmp_db()
    try:
        import sqlite3

        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE flags (name TEXT, value INTEGER)")
            conn.execute("INSERT INTO flags (name, value) VALUES (?, ?)", ("ready", 1))
        dep = SqliteDependency(db_path, "SELECT name, value FROM flags")
        assert dep.satisfied() == DependencyState.SATISFIED
    finally:
        pathlib.Path(db_path).unlink(missing_ok=True)


# ---- Save and load ----


def test_sqlite_dependency_save():
    """Save produces dict with type, db_path, query, params, timeout_seconds."""
    db_path = _tmp_db()
    try:
        dep = SqliteDependency(db_path, "SELECT 1", params=(1,), timeout_seconds=3.0)
        scope = LocalScope()
        context = MemoryContext(scope=scope, job_registry=SQLiteJobRegistry(":memory:"))
        saved = dep.save(context)

        assert saved["type"] == "SqliteDependency"
        assert saved["db_path"] == db_path
        assert saved["query"] == "SELECT 1"
        assert saved["params"] == (1,)
        assert saved["timeout_seconds"] == 3.0
    finally:
        pathlib.Path(db_path).unlink(missing_ok=True)


def test_sqlite_dependency_load():
    """Load reconstructs dependency from saved dict."""
    db_path = _tmp_db()
    try:
        import sqlite3

        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE t (x INTEGER)")
            conn.execute("INSERT INTO t (x) VALUES (42)")
        data = {
            "type": "SqliteDependency",
            "db_path": db_path,
            "query": "SELECT x FROM t",
            "params": None,
            "timeout_seconds": 5.0,
        }
        scope = LocalScope()
        context = MemoryContext(scope=scope, job_registry=SQLiteJobRegistry(":memory:"))
        loaded = SqliteDependency.load(context, data)

        assert loaded.db_path == db_path
        assert loaded.query == "SELECT x FROM t"
        assert loaded.params is None
        assert loaded.timeout_seconds == 5.0
        assert loaded.satisfied() == DependencyState.SATISFIED
    finally:
        pathlib.Path(db_path).unlink(missing_ok=True)


def test_sqlite_dependency_save_load_roundtrip():
    """Save then load preserves behaviour (same satisfied() result)."""
    db_path = _tmp_db()
    try:
        import sqlite3

        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE gate (status TEXT)")
            conn.execute("INSERT INTO gate (status) VALUES (?)", ("satisfied",))

        dep = SqliteDependency(db_path, "SELECT status FROM gate", params=())
        scope = LocalScope()
        context = MemoryContext(scope=scope, job_registry=SQLiteJobRegistry(":memory:"))
        saved = dep.save(context)
        loaded = SqliteDependency.load(context, saved)

        assert loaded.satisfied() == dep.satisfied() == DependencyState.SATISFIED
        assert loaded.db_path == dep.db_path
        assert loaded.query == dep.query
        assert loaded.params == dep.params
    finally:
        pathlib.Path(db_path).unlink(missing_ok=True)


def test_sqlite_dependency_load_with_params_tuple():
    """Load accepts params as tuple (e.g. from in-memory save)."""
    db_path = _tmp_db()
    try:
        import sqlite3

        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE t (id INTEGER)")
            conn.execute("INSERT INTO t (id) VALUES (?)", (10,))
        data = {
            "type": "SqliteDependency",
            "db_path": db_path,
            "query": "SELECT id FROM t WHERE id = ?",
            "params": (10,),
            "timeout_seconds": 2.0,
        }
        scope = LocalScope()
        context = MemoryContext(scope=scope, job_registry=SQLiteJobRegistry(":memory:"))
        loaded = SqliteDependency.load(context, data)
        assert loaded.satisfied() == DependencyState.SATISFIED
    finally:
        pathlib.Path(db_path).unlink(missing_ok=True)
