from collections.abc import Mapping
from typing import Any, Self
import uuid

from zahir.base_types import Context, Dependency, DependencyResult, DependencyState


class Semaphore(Dependency):
    """A dependency with state that can be updated
    directly. Can be used to pause jobs until some external
    condition is met, or to signal that the jobs are now impossible and starting them should not be attempted.

    Uses context.state to coordinate across multiple processes via a unique semaphore instance ID.
    """

    def __init__(self, context: Context, initial_state: DependencyState = DependencyState.SATISFIED) -> None:
        # Unique ID for this semaphore instance to coordinate across processes
        self.semaphore_id = str(uuid.uuid4())
        self.initial_state = initial_state
        self.context = context

    def _get_state_key(self) -> str:
        """Get the key for storing this semaphore's state in context.state."""
        return f"_semaphore_{self.semaphore_id}"

    def satisfied(self) -> DependencyResult:
        """Return the current state of the semaphore."""
        state_key = self._get_state_key()
        if state_key in self.context.state:
            state_value = self.context.state[state_key]
            return DependencyResult(state=DependencyState(state_value))

        return DependencyResult(state=self.initial_state)

    def open(self) -> None:
        """Set the semaphore to satisfied."""
        self.initial_state = DependencyState.SATISFIED
        state_key = self._get_state_key()
        self.context.state[state_key] = DependencyState.SATISFIED.value

    def close(self) -> None:
        """Set the semaphore to unsatisfied."""
        self.initial_state = DependencyState.UNSATISFIED
        state_key = self._get_state_key()
        self.context.state[state_key] = DependencyState.UNSATISFIED.value

    def abort(self) -> None:
        """Set the semaphore to impossible."""
        self.initial_state = DependencyState.IMPOSSIBLE
        state_key = self._get_state_key()
        self.context.state[state_key] = DependencyState.IMPOSSIBLE.value

    def request_extension(self, extra_seconds: float) -> Self:
        """Semaphores do not support time-based extensions; return self unchanged."""

        return self

    def save(self, context: Context) -> dict[str, Any]:
        """Save the semaphore to a dictionary."""
        # Get the current state from context.state
        result = self.satisfied()

        return {
            "type": "Semaphore",
            "semaphore_id": self.semaphore_id,
            "state": result.state.value,
        }

    @classmethod
    def load(cls, context: Context, data: Mapping[str, Any]) -> "Semaphore":
        """Load the semaphore from a dictionary."""

        state = DependencyState(data["state"])
        semaphore = cls(context=context, initial_state=state)

        # Restore the semaphore ID for cross-process coordination
        if "semaphore_id" in data:
            semaphore.semaphore_id = data["semaphore_id"]

        # Initialize the state in context.state
        state_key = semaphore._get_state_key()
        if state_key not in context.state:
            context.state[state_key] = state.value

        return semaphore
