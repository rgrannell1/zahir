from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Self, TypedDict

from zahir.base_types import (
    Dependency,
    DependencyResult,
    DependencyState,
    propagate_message_override,
    restore_message_override,
    serialise_message_override,
)


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

    DEFAULT_MESSAGE: dict[DependencyState, str] = {
        DependencyState.SATISFIED: "Time window satisfied (after={after}, before={before}).",
        DependencyState.UNSATISFIED: "Not yet time (after={after}, before={before}).",
        DependencyState.IMPOSSIBLE: "Time window has passed (before={before}).",
    }

    # What date must this job run before?
    before: datetime | None
    # What date must this job run after?
    after: datetime | None
    # Do we allow time-extensions?
    allow_extensions: ExtensionMode

    def __init__(
        self,
        before: datetime | None = None,
        after: datetime | None = None,
        allow_extensions: ExtensionMode = ExtensionMode.BOTH,
    ) -> None:
        self.before = before
        self.after = after
        self.allow_extensions = allow_extensions

    @classmethod
    def seconds_from_now(cls, seconds: float) -> "TimeDependency":
        """Create a TimeDependency that is satisfied after `seconds` seconds from now.

        @param seconds: Number of seconds from now.

        @return: A TimeDependency that is satisfied after `seconds` seconds from now.
        """

        after = datetime.now(tz=UTC) + timedelta(seconds=seconds)
        return cls(after=after)

    def satisfied(self) -> DependencyResult:
        """Check whether the time dependency is satisfied."""

        metadata = {
            "before": self.before.isoformat() if self.before else None,
            "after": self.after.isoformat() if self.after else None,
        }

        if not self.before and not self.after:
            # trivially true.
            state = DependencyState.SATISFIED
            return DependencyResult(
                type="TimeDependency",
                state=state,
                message=self.render_message(state, metadata),
                metadata=metadata,
            )

        now = datetime.now(tz=UTC)

        if self.before and now >= self.before:
            # time moves forward, this dependency can now never be met.
            state = DependencyState.IMPOSSIBLE
            return DependencyResult(
                type="TimeDependency",
                state=state,
                message=self.render_message(state, metadata),
                metadata=metadata,
            )

        if self.after:
            if now >= self.after:
                state = DependencyState.SATISFIED
                return DependencyResult(
                    type="TimeDependency",
                    state=state,
                    message=self.render_message(state, metadata),
                    metadata=metadata,
                )
            state = DependencyState.UNSATISFIED
            return DependencyResult(
                type="TimeDependency",
                state=state,
                message=self.render_message(state, metadata),
                metadata=metadata,
            )

        state = DependencyState.SATISFIED
        return DependencyResult(
            type="TimeDependency",
            state=state,
            message=self.render_message(state, metadata),
            metadata=metadata,
        )

    def request_extension(self, extra_seconds: float) -> Self:
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

        extended = type(self)(before=new_before, after=new_after, allow_extensions=self.allow_extensions)
        propagate_message_override(self, extended)
        return extended

    def save(self, context) -> Mapping[str, Any]:
        result: dict[str, Any] = {
            "type": "TimeDependency",
            "before": self.before.isoformat() if self.before else None,
            "after": self.after.isoformat() if self.after else None,
        }
        override_data = serialise_message_override(self)
        if override_data:
            result["message_override"] = override_data
        return result

    @classmethod
    def load(cls, context, data: Mapping[str, Any]) -> "TimeDependency":
        before = datetime.fromisoformat(data["before"]) if data["before"] else None
        after = datetime.fromisoformat(data["after"]) if data["after"] else None

        instance = cls(before=before, after=after)
        restore_message_override(instance, data)
        return instance
