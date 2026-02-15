from collections.abc import Mapping
import pathlib
import sqlite3
from typing import Any, Self

from zahir.base_types import Context, Dependency, DependencyResult, DependencyState

# Match job registry: safe-ish in practice for concurrent access
_DEFAULT_TIMEOUT_SECONDS = 5.0
_BUSY_TIMEOUT_MS = 5000


def _connect(db_path: str, timeout_seconds: float) -> sqlite3.Connection:
    """Open a connection with the same durability settings as SQLiteJobRegistry."""

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


class SqliteDependency(Dependency):
    """This class has two modes based on what's returned:

    - if it's one row x one column named status, use that to determine the state
    - otherwise use the cardinality of the result-set to determine the state; zero means unsatisfied,
        more means satisfied. Impossible cannot be represented in this mode apart from SQL erroring.
    """

    def _validate_db_path(self, db_path: str) -> None:
        """Check as early as possible that the database path is valid."""

        if not db_path or db_path.strip() == "":
            raise ValueError("db_path is required")

        if db_path == ":memory:":
            return

        if not pathlib.Path(db_path).exists():
            raise FileNotFoundError(f"db_path {db_path} does not exist")

    def __init__(
        self,
        db_path: str,
        query: str,
        params: tuple[Any, ...] | None = None,
        timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        self._validate_db_path(db_path)
        self.db_path = db_path
        self.query = query
        self.params = params
        self.timeout_seconds = timeout_seconds

    def satisfied(self) -> DependencyResult:
        """Check whether the SQLite dependency is satisfied."""

        with _connect(self.db_path, self.timeout_seconds) as conn:
            cursor = conn.cursor()
            cursor.execute(self.query, self.params or ())
            column_names = [description[0] for description in cursor.description] if cursor.description else []
            result = cursor.fetchone()

            # in either mode, missing results map to unsatisfied
            if result is None:
                return DependencyResult(type="SqliteDependency", state=DependencyState.UNSATISFIED)

            # one row × one column named "status" → use value as state
            if len(result) == 1 and column_names == ["status"]:
                status = result[0].lower().strip()

                match status:
                    case "satisfied":
                        return DependencyResult(type="SqliteDependency", state=DependencyState.SATISFIED)
                    case "unsatisfied":
                        return DependencyResult(type="SqliteDependency", state=DependencyState.UNSATISFIED)
                    case "impossible":
                        return DependencyResult(type="SqliteDependency", state=DependencyState.IMPOSSIBLE)
                    case _:
                        # maybe impossible? but this is a programmer fuckup so probably not.
                        raise ValueError(f"Invalid status: {status}")

            state = DependencyState.SATISFIED if len(result) > 0 else DependencyState.UNSATISFIED
            return DependencyResult(type="SqliteDependency", state=state)

    def save(self, context: Context) -> dict[str, Any]:
        """Save the SQLite dependency to a dictionary."""

        return {
            "type": "SqliteDependency",
            "db_path": self.db_path,
            "query": self.query,
            "params": self.params,
            "timeout_seconds": self.timeout_seconds,
        }

    @classmethod
    def load(cls, context: Context, data: Mapping[str, Any]) -> "SqliteDependency":
        """Load the SQLite dependency from a dictionary."""

        return cls(
            db_path=data["db_path"],
            query=data["query"],
            params=data["params"],
            timeout_seconds=data["timeout_seconds"],
        )

    def request_extension(self, extra_seconds: float) -> Self:
        """Beyond messing with parameters we cannot do much; just return self & allow overrides."""

        return self
