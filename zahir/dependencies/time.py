from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Self, TypedDict

from zahir.base_types import Dependency, DependencyState


class TimeDependencyData(TypedDict, total=False):
    """Serialized structure for TimeDependency."""

    before: str | None
    after: str | None


class ExtensionMode(StrEnum):
    """Strictness modes for time dependencies."""

    # We allow neither before or after to be extended.
    NONE = "none"
    # We allow only before to be extended.
    BEFORE = "before"
    # We allow only after to be extended.
    AFTER = "after"
    # We allow both before and after to be extended.
    BOTH = "both"

class TimeDependency(Dependency):
    """A dependency on a particular time-range.

    Once `before` is passed, the dependency is now considered impossible.
    """

    # What date must this job run before?
    before: datetime | None
    # What date must this job run after?
    after: datetime | None
    # Do we allow time-extensions?
    allow_extensions: ExtensionMode

    def __init__(self, before: datetime | None = None, after: datetime | None = None, allow_extensions: ExtensionMode = ExtensionMode.BOTH) -> None:
        self.before = before
        self.after = after
        self.allow_extensions = allow_extensions

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

    def request_extenstion(self, extra_seconds: float) -> Self:
        """Ask for a time-extension and return the resulting TimeDependency. This
        allows the 'before' and/or 'after' times to be pushed further into the future. We need this
        in order to honour time-dependencies while still allowing for job retries.

        @param extra_seconds: Number of seconds to extend the time-dependency by.

        @return: A new TimeDependency with the extended times, if the extension is allowed by the original instance.
        """

        new_before = self.before
        new_after = self.after

        if self.allow_extensions in {ExtensionMode.BEFORE, ExtensionMode.BOTH} and self.before:
            # Extend the 'before' time further into the future by `extra_seconds`

            new_before = self.before + timedelta(seconds=extra_seconds)

        if self.allow_extensions in {ExtensionMode.AFTER, ExtensionMode.BOTH} and self.after:
            # Extend the 'after' time further into the future by `extra_seconds`

            new_after = self.after + timedelta(seconds=extra_seconds)

        return type(self)(before=new_before, after=new_after, allow_extensions=self.allow_extensions)

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
