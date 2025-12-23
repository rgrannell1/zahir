from datetime import datetime
from zahir.types import Dependency, DependencyState


class TimeDependency(Dependency):
    """A dependency on a particular time-range.

    Once `before` is passed, the dependency is now considered impossible.
    """

    before: datetime | None
    after: datetime | None

    def __init__(self, before: datetime | None, after: datetime | None) -> None:
        self.before = before
        self.after = after

    def satisfied(self) -> DependencyState:
        """Check whether the time dependency is satisfied."""

        if not self.before and not self.after:
            # trivially true.
            return DependencyState.SATISFIED

        now = datetime.now()

        if self.before:
            # time moves forward, this dependency can now never be met.
            if now >= self.before:
                return DependencyState.IMPOSSIBLE

        if self.after:
            if now >= self.after:
                return DependencyState.SATISFIED
            else:
                return DependencyState.UNSATISFIED

        return DependencyState.SATISFIED
