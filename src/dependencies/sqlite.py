import pathlib
import sqlite3
from collections.abc import Generator
from contextlib import closing
from typing import Any

from tertius import ESleep

from constants import DEPENDENCY_DELAY_MS, IMPOSSIBLE, SATISFIED, UNSATISFIED
from effects import EImpossible, ESatisfied

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
    conn.execute("PRAGMA busy_timeout=%d;" % _BUSY_TIMEOUT_MS)
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


def _status_result(
    status: str,
    db_path: str,
    metadata: dict[str, Any],
) -> Generator[
    ESatisfied | EImpossible | ESleep, None, ESatisfied | EImpossible | None
]:
    if status == SATISFIED:
        event = ESatisfied(metadata=metadata)
        yield event
        return event
    elif status == IMPOSSIBLE:
        event = EImpossible(reason=f"status=impossible for query against {db_path}")
        yield event
        return event
    elif status == UNSATISFIED:
        yield ESleep(ms=DEPENDENCY_DELAY_MS)
        return None


def sqlite_dependency(
    db_path: str,
    query: str,
    params: tuple[Any, ...] | None = None,
    timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
) -> Generator[ESatisfied | EImpossible | ESleep, None, ESatisfied | EImpossible | None]:
    _validate_db_path(db_path)

    metadata = {
        "db_path": db_path,
        "query": query,
        "params": params,
        "timeout_seconds": timeout_seconds,
    }

    while True:
        column_names, row = _query(db_path, query, params or (), timeout_seconds)

        if row is None:
            yield ESleep(ms=DEPENDENCY_DELAY_MS)
            continue

        if len(row) == 1 and column_names == ["status"]:
            status = _parse_status(row[0])
            if status == UNSATISFIED:
                yield ESleep(ms=DEPENDENCY_DELAY_MS)
                continue
            return (yield from _status_result(status, db_path, metadata))

        event = ESatisfied(metadata=metadata)
        yield event
        return event
