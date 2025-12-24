from threading import Lock
from types import TracebackType
from typing import Any, Self, TypedDict
from zahir.types import Dependency, DependencyState


class ConcurrencyLimitData(TypedDict):
    """Serialized structure for ConcurrencyLimit."""
    
    limit: int
    claimed: int


class ConcurrencyLimit(Dependency):
    """Limit the number of jobs doing something concurrently at once."""

    limit: int
    claimed: int = 0

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self._lock = Lock()

    def claim(self) -> None:
        """Claim a slot in the concurrency limit."""

        with self._lock:
            self.claimed += 1

    def free(self) -> None:
        """Free a slot in the concurrency limit."""

        with self._lock:
            self.claimed -= 1
            self.claimed = max(0, self.claimed)

    def satisfied(self) -> DependencyState:
        """Check whether the concurrency limit is satisfied."""

        with self._lock:
            return (
                DependencyState.SATISFIED
                if self.claimed < self.limit
                else DependencyState.UNSATISFIED
            )

    def save(self) -> dict[str, Any]:
        """Save the concurrency limit to a dictionary."""

        return {
            "limit": self.limit,
            # This will be reset between serialisations
            "claimed": 0,
        }

    @classmethod
    def load(cls, context, data: dict[str, Any]) -> Self:
        """Load the concurrency limit from a dictionary."""

        return cls(limit=data["limit"])

    def __enter__(self) -> "ConcurrencyLimit":
        """Enter the context manager by claiming a slot."""

        self.claim()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit the context manager by freeing a slot."""

        self.free()
        return None
