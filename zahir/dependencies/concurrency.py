from collections.abc import Mapping
from multiprocessing import BoundedSemaphore
from typing import Any, Self

from zahir.base_types import Dependency, DependencyState


class ConcurrencyLimit(Dependency):
    """Limit the number of jobs running concurrently."""

    def __init__(self, limit: int, slots: int) -> None:
        self.limit = limit
        self.slots = slots
        self._semaphore = BoundedSemaphore(self.limit)

    def satisfied(self) -> DependencyState:
        acquired = 0
        for _ in range(self.slots):
            if not self._semaphore.acquire(block=False):
                for _ in range(acquired):
                    self._semaphore.release()
                return DependencyState.UNSATISFIED
            acquired += 1
        return DependencyState.SATISFIED

    def request_extension(self, extra_seconds: float) -> Self:
        """ConcurrencyLimit does not support extensions, return self unchanged."""
        return self

    def __enter__(self) -> "ConcurrencyLimit":
        """When entering the context, we assume the slots have already been claimed via satisfied()."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """On exit, release the slots."""
        for _ in range(self.slots):
            self._semaphore.release()

    def save(self) -> Mapping[str, Any]:
        """Save the concurrency limit configuration (not the semaphore state)."""
        return {
            "type": "ConcurrencyLimit",
            "limit": self.limit,
            "slots": self.slots,
        }

    @classmethod
    def load(cls, context, data: Mapping[str, Any]) -> Self:
        """Load the concurrency limit configuration and recreate the semaphore."""
        return cls(limit=data["limit"], slots=data["slots"])
