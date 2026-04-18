import sqlite3
import tempfile
import pathlib

import pytest

from dependencies.sqlite import sqlite_dependency
from effects import EImpossible, ESatisfied
from tertius import ESleep


def _make_db(rows: list[tuple]) -> str:
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    conn = sqlite3.connect(tmp.name)
    conn.execute("CREATE TABLE state (status TEXT)")
    conn.executemany("INSERT INTO state VALUES (?)", rows)
    conn.commit()
    conn.close()
    return tmp.name


def test_non_status_column_yields_satisfied():
    """Proves a row with a non-status column name yields ESatisfied immediately."""

    db = _make_db([("anything",)])
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE items (value TEXT)")
    conn.execute("INSERT INTO items VALUES ('something')")
    conn.commit()
    conn.close()

    gen = sqlite_dependency(db, "SELECT value FROM items", ())
    assert isinstance(next(gen), ESatisfied)


def test_status_satisfied_yields_satisfied():
    """Proves a status=satisfied row yields ESatisfied."""

    db = _make_db([("satisfied",)])
    gen = sqlite_dependency(db, "SELECT status FROM state", ())
    assert isinstance(next(gen), ESatisfied)


def test_status_impossible_yields_impossible():
    """Proves a status=impossible row yields EImpossible."""

    db = _make_db([("impossible",)])
    gen = sqlite_dependency(db, "SELECT status FROM state", ())
    assert isinstance(next(gen), EImpossible)


def test_status_unsatisfied_yields_sleep():
    """Proves a status=unsatisfied row yields ESleep."""

    db = _make_db([("unsatisfied",)])
    gen = sqlite_dependency(db, "SELECT status FROM state", ())
    assert isinstance(next(gen), ESleep)


def test_no_rows_yields_sleep():
    """Proves an empty result set yields ESleep."""

    db = _make_db([])
    gen = sqlite_dependency(db, "SELECT status FROM state", ())
    assert isinstance(next(gen), ESleep)


def test_satisfied_metadata_includes_query_and_path():
    """Proves ESatisfied metadata contains the db_path and query."""

    db = _make_db([("satisfied",)])
    gen = sqlite_dependency(db, "SELECT status FROM state", ())
    effect = next(gen)

    assert effect.metadata["db_path"] == db
    assert effect.metadata["query"] == "SELECT status FROM state"


def test_invalid_status_value_raises():
    """Proves an unrecognised status value raises ValueError."""

    db = _make_db([("pending",)])
    gen = sqlite_dependency(db, "SELECT status FROM state", ())

    with pytest.raises(ValueError, match="invalid status value"):
        next(gen)


def test_empty_db_path_raises():
    """Proves an empty db_path raises ValueError."""

    with pytest.raises(ValueError):
        next(sqlite_dependency("", "SELECT 1", ()))


def test_missing_db_path_raises():
    """Proves a non-existent db_path raises FileNotFoundError."""

    with pytest.raises(FileNotFoundError):
        next(sqlite_dependency("/tmp/does_not_exist.db", "SELECT 1", ()))


def test_query_params_are_passed_through():
    """Proves query parameters filter results correctly."""

    db = _make_db([("satisfied",), ("impossible",)])
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE items (name TEXT, status TEXT)")
    conn.execute("INSERT INTO items VALUES ('a', 'satisfied')")
    conn.commit()
    conn.close()

    gen = sqlite_dependency(db, "SELECT status FROM items WHERE name = ?", ("a",))
    assert isinstance(next(gen), ESatisfied)
