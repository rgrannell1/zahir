import sqlite3
import tempfile
import pathlib

import pytest

from tertius import EEmit, ESleep

from zahir.core.dependencies.dependency import ImpossibleError
from zahir.core.dependencies.sqlite import _sqlite_condition, sqlite_dependency


def _make_db(rows: list[tuple]) -> str:
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    conn = sqlite3.connect(tmp.name)
    conn.execute("CREATE TABLE state (status TEXT)")
    conn.executemany("INSERT INTO state VALUES (?)", rows)
    conn.commit()
    conn.close()
    return tmp.name


def test_non_status_column_emits_satisfied():
    """Proves a row with a non-status column name emits satisfied immediately."""

    db = _make_db([("anything",)])
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE items (value TEXT)")
    conn.execute("INSERT INTO items VALUES ('something')")
    conn.commit()
    conn.close()

    emit = next(sqlite_dependency(db, "SELECT value FROM items", ()))
    assert isinstance(emit, EEmit)
    assert emit.body[0] == "satisfied"


def test_status_satisfied_emits_satisfied():
    """Proves a status=satisfied row emits satisfied."""

    db = _make_db([("satisfied",)])
    emit = next(sqlite_dependency(db, "SELECT status FROM state", ()))
    assert emit.body[0] == "satisfied"


def test_status_impossible_emits_impossible():
    """Proves a status=impossible row emits impossible."""

    db = _make_db([("impossible",)])
    emit = next(sqlite_dependency(db, "SELECT status FROM state", ()))
    assert emit.body[0] == "impossible"


def test_status_unsatisfied_yields_sleep():
    """Proves a status=unsatisfied row yields ESleep."""

    db = _make_db([("unsatisfied",)])
    assert isinstance(
        next(sqlite_dependency(db, "SELECT status FROM state", ())), ESleep
    )


def test_no_rows_yields_sleep():
    """Proves an empty result set yields ESleep."""

    db = _make_db([])
    assert isinstance(
        next(sqlite_dependency(db, "SELECT status FROM state", ())), ESleep
    )


def test_satisfied_metadata_includes_query_and_path():
    """Proves the satisfied body contains the db_path and query."""

    db = _make_db([("satisfied",)])
    emit = next(sqlite_dependency(db, "SELECT status FROM state", ()))

    assert emit.body[0] == "satisfied"
    assert emit.body[1]["db_path"] == db
    assert emit.body[1]["query"] == "SELECT status FROM state"


def test_invalid_status_value_raises():
    """Proves an unrecognised status value raises ValueError."""

    db = _make_db([("pending",)])
    with pytest.raises(ValueError, match="invalid status value"):
        next(sqlite_dependency(db, "SELECT status FROM state", ()))


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

    emit = next(
        sqlite_dependency(db, "SELECT status FROM items WHERE name = ?", ("a",))
    )
    assert emit.body[0] == "satisfied"


# return values


def test_satisfied_status_returns_tuple_as_generator_value():
    """Proves the generator returns the satisfied tuple as its StopIteration value."""

    db = _make_db([("satisfied",)])
    gen = sqlite_dependency(db, "SELECT status FROM state", ())
    emit = next(gen)
    with pytest.raises(StopIteration) as exc:
        next(gen)
    assert exc.value.value is emit.body


def test_impossible_status_returns_tuple_as_generator_value():
    """Proves the generator returns the impossible tuple as its StopIteration value."""

    db = _make_db([("impossible",)])
    gen = sqlite_dependency(db, "SELECT status FROM state", ())
    emit = next(gen)
    with pytest.raises(StopIteration) as exc:
        next(gen)
    assert exc.value.value is emit.body


def test_non_status_column_returns_tuple_as_generator_value():
    """Proves the generator returns the satisfied tuple for non-status column queries."""

    db = _make_db([])
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE items (value TEXT)")
    conn.execute("INSERT INTO items VALUES ('x')")
    conn.commit()
    conn.close()

    gen = sqlite_dependency(db, "SELECT value FROM items", ())
    emit = next(gen)
    with pytest.raises(StopIteration) as exc:
        next(gen)
    assert exc.value.value is emit.body


def test_unsatisfied_then_satisfied_loops_correctly():
    """Proves the generator loops past unsatisfied status and retries."""

    db = _make_db([("unsatisfied",)])
    gen = sqlite_dependency(db, "SELECT status FROM state", ())
    assert isinstance(next(gen), ESleep)

    conn = sqlite3.connect(db)
    conn.execute("UPDATE state SET status = 'satisfied'")
    conn.commit()
    conn.close()

    emit = next(gen)
    assert emit.body[0] == "satisfied"


def test_no_rows_loops_and_retries_when_row_appears():
    """Proves the generator loops past an empty result set and retries when a row appears."""

    db = _make_db([])
    gen = sqlite_dependency(db, "SELECT status FROM state", ())
    assert isinstance(next(gen), ESleep)

    conn = sqlite3.connect(db)
    conn.execute("INSERT INTO state VALUES ('satisfied')")
    conn.commit()
    conn.close()

    emit = next(gen)
    assert emit.body[0] == "satisfied"


# _sqlite_condition — unsatisfied / impossible branches


def test_sqlite_condition_unsatisfied_returns_false():
    """Proves _sqlite_condition returns False for a status=unsatisfied row."""

    db = _make_db([("unsatisfied",)])
    with pytest.raises(StopIteration) as exc:
        next(_sqlite_condition(db, "SELECT status FROM state", None, 5.0))
    assert exc.value.value is False


def test_sqlite_condition_impossible_raises():
    """Proves _sqlite_condition raises ImpossibleError for a status=impossible row."""

    db = _make_db([("impossible",)])
    with pytest.raises(ImpossibleError):
        next(_sqlite_condition(db, "SELECT status FROM state", None, 5.0))
