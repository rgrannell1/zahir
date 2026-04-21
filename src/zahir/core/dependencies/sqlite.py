import pathlib
import sqlite3
from collections.abc import Generator
from contextlib import closing
from typing import Any

from zahir.core.constants import IMPOSSIBLE, SATISFIED, UNSATISFIED
from zahir.core.dependencies.dependency import (
    DependencyResult,
    ImpossibleError,
    dependency,
)

_DEFAULT_TIMEOUT_SECONDS = 5.0
_BUSY_TIMEOUT_MS = 5000


def _connect(db_path: str, timeout_seconds: float) -> sqlite3.Connection:
    conn = sqlite3.connect(
        db_path,
        timeout=timeout_seconds,
        isolation_level=None,
        check_same_thread=False,
    )
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS};")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def _validate_db_path(db_path: str) -> None:
    if not db_path or db_path.strip() == "":
        raise ValueError("db_path is required")
    if db_path == ":memory:":
        return
    if not pathlib.Path(db_path).exists():
        raise FileNotFoundError(f"db_path {db_path} does not exist")


def _query(
    db_path: str,
    query: str,
    params: tuple[Any, ...],
    timeout_seconds: float,
) -> tuple[list[str], tuple[Any, ...] | None]:
    with closing(_connect(db_path, timeout_seconds)) as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        column_names = (
            [name for name, *_ in cursor.description] if cursor.description else []
        )
        return column_names, cursor.fetchone()


def _parse_status(raw: str) -> str:
    status = raw.lower().strip()
    if status not in {SATISFIED, UNSATISFIED, IMPOSSIBLE}:
        raise ValueError(f"invalid status value: {status!r}")
    return status


def _sqlite_condition(
    db_path: str,
    query: str,
    params: tuple[Any, ...] | None,
    timeout_seconds: float,
) -> Generator[Any, Any, Any]:
    """Returns (True, metadata) if the query returns rows (or a satisfied status), False if not yet, raises ImpossibleError if impossible."""
    metadata = {
        "db_path": db_path,
        "query": query,
        "params": params,
        "timeout_seconds": timeout_seconds,
    }
    column_names, row = _query(db_path, query, params or (), timeout_seconds)

    if row is None:
        return False

    if len(row) == 1 and column_names == ["status"]:
        status = _parse_status(row[0])
        if status == UNSATISFIED:
            return False
        if status == IMPOSSIBLE:
            raise ImpossibleError(f"status=impossible for query against {db_path}")

    return (True, metadata)
    yield  # make it a generator function


def sqlite_dependency(
    db_path: str,
    query: str,
    params: tuple[Any, ...] | None = None,
    timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
) -> Generator[Any, Any, DependencyResult]:
    _validate_db_path(db_path)
    return dependency(
        lambda: _sqlite_condition(db_path, query, params, timeout_seconds)
    )
