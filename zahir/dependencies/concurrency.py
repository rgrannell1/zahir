from collections.abc import Mapping
from typing import Any, Self
import uuid

from zahir.base_types import Context, Dependency, DependencyState


class ConcurrencyLimit(Dependency):
    """Limit the number of jobs running concurrently. Coordinated via a BoundedSemaphore
    stored in the context's state. Jobs consuming concurrency limit MUST
    use ConcurrencyLimit as a context manager to ensure proper release of slots.

    TODO allow dependencies to enter / exit on job start. Probably a good
    idea.
    """

    def __init__(self, limit: int, slots: int, context: Context, semaphore_id: str | None = None) -> None:
        self.limit = limit
        self.slots = slots
        self.context = context

        # Each instance of the concurrency limit gets a unique ID,
        # possibly as a parameter. This ID is used to store/retrieve
        # an central BoundedSemaphore in the context's `state``.
        self.instance_id = str(uuid.uuid4())
        self.semaphore_id = semaphore_id or self.instance_id
        self._semaphore = None
        self._ensure_semaphore()

    def _ensure_semaphore(self) -> None:
        # For the moment, set the semaphore in the context's state using a key
        state_key = f"_concurrency_semaphore_{self.semaphore_id}"

        if state_key not in self.context.state:
            self.context.state[state_key] = self.context.manager.BoundedSemaphore(self.limit)

        self._semaphore = self.context.state[state_key]

    def satisfied(self) -> DependencyState:
        """Check if concurrency limit is satisfied. Non-blocking acquire."""

        self._ensure_semaphore()

        if not self._semaphore:
            # Should also be impossible to reach this code-path
            raise NotImplementedError("Semaphore not initialized in context.")

        acquired = 0
        for _ in range(self.slots):
            # Try to acquire without blocking
            if not self._semaphore.acquire(blocking=False):
                # acquire failed; release what we've acquired so far
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
        if self._semaphore:
            for _ in range(self.slots):
                self._semaphore.release()

    def save(self, context) -> Mapping[str, Any]:
        """Save the concurrency limit configuration and semaphore to context."""
        self.context = context
        self._ensure_semaphore()

        return {
            "type": "ConcurrencyLimit",
            "limit": self.limit,
            "slots": self.slots,
            "semaphore_id": self.semaphore_id,
        }

    @classmethod
    def load(cls, context, data: Mapping[str, Any]) -> Self:
        """Load the concurrency limit configuration and retrieve semaphore from context."""
        semaphore_id = data.get("semaphore_id")
        instance = cls(limit=data["limit"], slots=data["slots"], semaphore_id=semaphore_id, context=context)
        instance._ensure_semaphore()

        return instance
