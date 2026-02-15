from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, Self, TypedDict

import psutil

from zahir.base_types import Dependency, DependencyResult, DependencyState

type ResourceType = Literal["cpu"] | Literal["memory"]


class ResourceLimitData(TypedDict, total=False):
    """Serialized structure for ResourceLimit."""

    resource: ResourceType
    max_percent: float
    timeout_at: str | None


class ResourceLimit(Dependency):
    """A dependency that checks machine-wide resource usage is below a threshold.

    Useful to block until system-resources recover (I have OOMed myself too
    often.)
    """

    def __init__(
        self,
        resource: ResourceType,
        max_percent: float,
        timeout: float | None = None,
    ) -> None:
        """Create a resource limit dependency.

        @param resource: The resource type to monitor ("cpu" or "memory")
        @param max_percent: Maximum usage percentage (0-100). Satisfied when usage <= this.
        @param timeout: Optional timeout in seconds. If set, dependency becomes IMPOSSIBLE after duration
        """
        self.resource = resource
        self.max_percent = max_percent

        if timeout is not None:
            self.timeout_at: datetime | None = datetime.now(tz=UTC) + timedelta(seconds=timeout)
        else:
            self.timeout_at = None

    @classmethod
    def cpu(cls, max_percent: float, timeout: float | None = None) -> "ResourceLimit":
        return cls("cpu", max_percent, timeout=timeout)

    @classmethod
    def memory(cls, max_percent: float, timeout: float | None = None) -> "ResourceLimit":
        return cls("memory", max_percent, timeout=timeout)

    def _get_usage(self) -> float:
        """Get the current resource usage as a percentage."""

        if self.resource == "cpu":
            return psutil.cpu_percent(interval=0.1)
        if self.resource == "memory":
            return psutil.virtual_memory().percent
        raise ValueError(f"Unknown resource type: {self.resource}")

    def satisfied(self) -> DependencyResult:
        """Check whether the resource usage is below the threshold."""

        if self.timeout_at is not None:
            now = datetime.now(tz=UTC)
            if now >= self.timeout_at:
                return DependencyResult(state=DependencyState.IMPOSSIBLE)

        if self._get_usage() <= self.max_percent:
            return DependencyResult(state=DependencyState.SATISFIED)

        return DependencyResult(state=DependencyState.UNSATISFIED)

    def request_extension(self, extra_seconds: float) -> Self:
        """Extend the timeout deadline if one is set.

        @param extra_seconds: Number of seconds to extend the timeout by.
        @return: A new ResourceLimit with the extended timeout.
        """
        if self.timeout_at is None:
            return self

        new_timeout_at = self.timeout_at + timedelta(seconds=extra_seconds)

        result = type(self)(resource=self.resource, max_percent=self.max_percent)
        result.timeout_at = new_timeout_at
        return result

    def save(self, context) -> Mapping[str, Any]:
        """Serialize the resource limit to a dictionary."""
        return {
            "type": "ResourceLimit",
            "resource": self.resource,
            "max_percent": self.max_percent,
            "timeout_at": self.timeout_at.isoformat() if self.timeout_at else None,
        }

    @classmethod
    def load(cls, context, data: Mapping[str, Any]) -> "ResourceLimit":
        resource: ResourceType = data["resource"]
        max_percent = data["max_percent"]
        timeout_at_str = data.get("timeout_at")

        instance = cls(resource=resource, max_percent=max_percent)

        if timeout_at_str:
            instance.timeout_at = datetime.fromisoformat(timeout_at_str)

        return instance
