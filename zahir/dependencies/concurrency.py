from types import TracebackType
from zahir.types import Dependency, DependencyState


class ConcurrencyLimit(Dependency):
    """Limit the number of jobs doing something concurrently at once."""

    limit: int
    claimed: int = 0

    def __init__(self, limit: int) -> None:
        self.limit = limit

    def claim(self) -> None:
        """Claim a slot in the concurrency limit."""

        self.claimed += 1

    def free(self) -> None:
        """Free a slot in the concurrency limit."""

        self.claimed -= 1
        self.claimed = max(0, self.claimed)

    def satisfied(self) -> DependencyState:
        """Check whether the concurrency limit is satisfied."""

        return (
            DependencyState.SATISFIED
            if self.claimed < self.limit
            else DependencyState.UNSATISFIED
        )

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
