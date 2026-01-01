from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, TypedDict

from zahir.base_types import Dependency, DependencyState


class TimeDependencyData(TypedDict, total=False):
    """Serialized structure for TimeDependency."""

    before: str | None
    after: str | None


class TimeDependency(Dependency):
    """A dependency on a particular time-range.

    Once `before` is passed, the dependency is now considered impossible.
    """

    # What date must this job run before?
    before: datetime | None
    # What date must this job run after?
    after: datetime | None

    def __init__(self, before: datetime | None = None, after: datetime | None = None) -> None:
        self.before = before
        self.after = after

    def satisfied(self) -> DependencyState:
        """Check whether the time dependency is satisfied."""

        if not self.before and not self.after:
            # trivially true.
            return DependencyState.SATISFIED

        now = datetime.now(tz=UTC)

        if self.before and now >= self.before:
            # time moves forward, this dependency can now never be met.
            return DependencyState.IMPOSSIBLE

        if self.after:
            if now >= self.after:
                return DependencyState.SATISFIED
            return DependencyState.UNSATISFIED

        return DependencyState.SATISFIED

    def save(self) -> Mapping[str, Any]:
        return {
            "type": "TimeDependency",
            "before": self.before.isoformat() if self.before else None,
            "after": self.after.isoformat() if self.after else None,
        }

    @classmethod
    def load(cls, context, data: Mapping[str, Any]) -> "TimeDependency":
        before = datetime.fromisoformat(data["before"]) if data["before"] else None
        after = datetime.fromisoformat(data["after"]) if data["after"] else None

        return cls(before=before, after=after)
