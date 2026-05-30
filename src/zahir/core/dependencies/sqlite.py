# Dependency that waits until a SQLite query returns a satisfied status row.
import pathlib
import sqlite3
from collections.abc import Generator
from contextlib import closing
from functools import partial
from typing import Any

from zahir.core.constants import DependencyState
from zahir.core.dependencies.dependency import check, dependency
from zahir.core.zahir_types import ConditionResult, DependencyResult

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
        column_names = [name for name, *_ in cursor.description] if cursor.description else []
        return column_names, cursor.fetchone()


def _parse_status(raw: str) -> str:
    status = raw.lower().strip()
    valid_statuses = {
        DependencyState.SATISFIED,
        DependencyState.UNSATISFIED,
        DependencyState.IMPOSSIBLE,
    }
    if status not in valid_statuses:
        raise ValueError(f"invalid status value: {status!r}")
    return status


def sqlite_condition(
    db_path: str,
    query: str,
    params: tuple[Any, ...] | None,
    timeout_seconds: float,
) -> Generator[Any, Any, ConditionResult]:
    """Returns satisfied if the query returns rows (or a satisfied status), unsatisfied if not yet, impossible if the status row is impossible."""  # noqa: E501
    metadata = {
        "db_path": db_path,
        "query": query,
        "params": params,
        "timeout_seconds": timeout_seconds,
    }
    column_names, row = _query(db_path, query, params or (), timeout_seconds)

    if row is None:
        return ("unsatisfied", metadata)

    if len(row) == 1 and column_names == ["status"]:
        status = _parse_status(row[0])
        if status == DependencyState.UNSATISFIED:
            return ("unsatisfied", metadata)
        if status == DependencyState.IMPOSSIBLE:
            return ("impossible", metadata)

    return ("satisfied", metadata)
    yield  # make it a generator function


def sqlite_dependency(  # noqa: PLR0913
    db_path: str,
    query: str,
    params: tuple[Any, ...] | None = None,
    connection_timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
    poll_timeout_ms: int | None = None,
) -> Generator[Any, Any, DependencyResult]:
    _validate_db_path(db_path)
    return dependency(
        partial(sqlite_condition, db_path, query, params, connection_timeout_seconds),
        timeout_ms=poll_timeout_ms,
        label=f"sqlite '{db_path}'",
    )


def check_sqlite_dependency(
    db_path: str,
    query: str,
    params: tuple[Any, ...] | None = None,
    connection_timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
) -> Generator[Any, Any, DependencyResult]:
    """Evaluate the sqlite condition once; return satisfied or impossible without retrying."""
    _validate_db_path(db_path)
    return check(
        partial(sqlite_condition, db_path, query, params, connection_timeout_seconds),
        label=f"sqlite '{db_path}'",
    )
