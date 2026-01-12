from collections.abc import Mapping
from typing import Any, Self

from zahir.base_types import Context, Dependency, DependencyState


class Semaphore(Dependency):
    """A dependency with state that can be updated
    directly. Can be used to pause jobs until some external
    condition is met, or to signal that the jobs are now impossible and starting them should not be attempted.
    """

    def __init__(self, initial_state: DependencyState = DependencyState.SATISFIED) -> None:
        self.state = initial_state

    def satisfied(self) -> DependencyState:
        """Return the current state of the semaphore."""

        return self.state

    def open(self) -> None:
        """Set the semaphore to satisfied."""

        self.state = DependencyState.SATISFIED

    def close(self) -> None:
        """Set the semaphore to unsatisfied."""

        self.state = DependencyState.UNSATISFIED

    def abort(self) -> None:
        """Set the semaphore to impossible."""

        self.state = DependencyState.IMPOSSIBLE

    def request_extension(self, extra_seconds: float) -> Self:
        """Semaphores do not support time-based extensions; return self unchanged."""

        return self

    def save(self, context) -> dict[str, Any]:
        """Save the semaphore to a dictionary."""

        return {
            "type": "Semaphore",
            "state": self.state.value,
        }

    @classmethod
    def load(cls, context: Context, data: Mapping[str, Any]) -> "Semaphore":
        """Load the semaphore from a dictionary."""

        state = DependencyState(data["state"])
        return cls(initial_state=state)
