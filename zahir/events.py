"""Events

Workflows should be observable. So we'll yield events describing the state of the workflow engine over time. The runner can dispatch these events as desired.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from zahir.types import Job


class ZahirEvent(ABC):
    """Base class for all Zahir events"""

    @abstractmethod
    def save(self) -> dict[str, Any]:
        """Serialize the event to a dictionary.

        @return: The serialized event data
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def load(cls, data: dict[str, Any]) -> "ZahirEvent":
        """Deserialize the event from a dictionary.

        @param data: The serialized event data
        @return: The deserialized event
        """
        raise NotImplementedError


@dataclass
class WorkflowCompleteEvent(ZahirEvent):
    """Indicates that the workflow has completed"""

    workflow_id: str
    duration_seconds: float

    def save(self) -> dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "duration_seconds": self.duration_seconds,
        }

    @classmethod
    def load(cls, data: dict[str, Any]) -> "WorkflowCompleteEvent":
        return cls(
            workflow_id=data["workflow_id"], duration_seconds=data["duration_seconds"]
        )


@dataclass
class WorkflowStallStartEvent(ZahirEvent):
    """Indicates that the workflow is starting a stall period"""

    workflow_id: str
    stall_duration_seconds: float

    def save(self) -> dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "stall_duration_seconds": self.stall_duration_seconds,
        }

    @classmethod
    def load(cls, data: dict[str, Any]) -> "WorkflowStallStartEvent":
        return cls(
            workflow_id=data["workflow_id"],
            stall_duration_seconds=data["stall_duration_seconds"],
        )


@dataclass
class WorkflowStallEndEvent(ZahirEvent):
    """Indicates that the workflow has finished a stall period"""

    workflow_id: str
    stall_duration_seconds: float

    def save(self) -> dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "stall_duration_seconds": self.stall_duration_seconds,
        }

    @classmethod
    def load(cls, data: dict[str, Any]) -> "WorkflowStallEndEvent":
        return cls(
            workflow_id=data["workflow_id"],
            stall_duration_seconds=data["stall_duration_seconds"],
        )


@dataclass
class JobRunnableEvent(ZahirEvent):
    """Indicates that a job is runnable"""

    workflow_id: str
    job: "Job"
    job_id: str

    def save(self) -> dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "job_type": type(self.job).__name__,
        }

    @classmethod
    def load(cls, data: dict[str, Any]) -> "JobRunnableEvent":
        # Job is not deserialized from events - set to None
        return cls(
            workflow_id=data["workflow_id"],
            job=None,  # type: ignore
            job_id=data["job_id"],
        )


@dataclass
class JobRunningEvent(ZahirEvent):
    """Indicates that a job is currently running"""

    workflow_id: str
    job: "Job"
    job_id: str

    def save(self) -> dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "job_type": type(self.job).__name__,
        }

    @classmethod
    def load(cls, data: dict[str, Any]) -> "JobRunningEvent":
        # Job is not deserialized from events - set to None
        return cls(
            workflow_id=data["workflow_id"],
            job=None,  # type: ignore
            job_id=data["job_id"],
        )


@dataclass
class JobCompletedEvent(ZahirEvent):
    """Indicates that a job has completed successfully"""

    workflow_id: str
    job: "Job"
    job_id: str
    duration_seconds: float

    def save(self) -> dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "job_type": type(self.job).__name__,
            "duration_seconds": self.duration_seconds,
        }

    @classmethod
    def load(cls, data: dict[str, Any]) -> "JobCompletedEvent":
        return cls(
            workflow_id=data["workflow_id"],
            job=None,  # type: ignore
            job_id=data["job_id"],
            duration_seconds=data["duration_seconds"],
        )


@dataclass
class JobStartedEvent(ZahirEvent):
    """Indicates that a job has started execution"""

    workflow_id: str
    job: "Job"
    job_id: str

    def save(self) -> dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "job_type": type(self.job).__name__,
        }

    @classmethod
    def load(cls, data: dict[str, Any]) -> "JobStartedEvent":
        return cls(
            workflow_id=data["workflow_id"],
            job=None,  # type: ignore
            job_id=data["job_id"],
        )


@dataclass
class JobTimeoutEvent(ZahirEvent):
    """Indicates that a job has timed out"""

    workflow_id: str
    job: "Job"
    job_id: str
    duration_seconds: float

    def save(self) -> dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "job_type": type(self.job).__name__,
            "duration_seconds": self.duration_seconds,
        }

    @classmethod
    def load(cls, data: dict[str, Any]) -> "JobTimeoutEvent":
        return cls(
            workflow_id=data["workflow_id"],
            job=None,  # type: ignore
            job_id=data["job_id"],
            duration_seconds=data["duration_seconds"],
        )


@dataclass
class JobRecoveryStarted(ZahirEvent):
    """Indicates that a job recovery has started"""

    workflow_id: str
    job: "Job"
    job_id: str

    def save(self) -> dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "job_type": type(self.job).__name__,
        }

    @classmethod
    def load(cls, data: dict[str, Any]) -> "JobRecoveryStarted":
        return cls(
            workflow_id=data["workflow_id"],
            job=None,  # type: ignore
            job_id=data["job_id"],
        )


@dataclass
class JobRecoveryCompleted(ZahirEvent):
    """Indicates that a job recovery has completed"""

    workflow_id: str
    job: "Job"
    job_id: str
    duration_seconds: float

    def save(self) -> dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "job_type": type(self.job).__name__,
            "duration_seconds": self.duration_seconds,
        }

    @classmethod
    def load(cls, data: dict[str, Any]) -> "JobRecoveryCompleted":
        return cls(
            workflow_id=data["workflow_id"],
            job=None,  # type: ignore
            job_id=data["job_id"],
            duration_seconds=data["duration_seconds"],
        )


@dataclass
class JobRecoveryTimeout(ZahirEvent):
    """Indicates that a job recovery has timed out"""

    workflow_id: str
    job: "Job"
    job_id: str

    def save(self) -> dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "job_type": type(self.job).__name__,
        }

    @classmethod
    def load(cls, data: dict[str, Any]) -> "JobRecoveryTimeout":
        return cls(
            workflow_id=data["workflow_id"],
            job=None,  # type: ignore
            job_id=data["job_id"],
        )


@dataclass
class JobIrrecoverableEvent(ZahirEvent):
    """Indicates that a job recovery has failed irrecoverably"""

    workflow_id: str
    error: Exception
    job: "Job"
    job_id: str

    def save(self) -> dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "job_type": type(self.job).__name__,
            "error": str(self.error),
            "error_type": type(self.error).__name__,
        }

    @classmethod
    def load(cls, data: dict[str, Any]) -> "JobIrrecoverableEvent":
        # Recreate a generic exception from the error string
        error_msg = data.get("error", "Unknown error")
        return cls(
            workflow_id=data["workflow_id"],
            error=Exception(error_msg),
            job=None,  # type: ignore
            job_id=data["job_id"],
        )


@dataclass
class JobPrecheckFailedEvent(ZahirEvent):
    """Indicates that a job's precheck validation failed"""

    workflow_id: str
    job: "Job"
    job_id: str
    errors: list[str]

    def save(self) -> dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "job_type": type(self.job).__name__,
            "errors": self.errors,
        }

    @classmethod
    def load(cls, data: dict[str, Any]) -> "JobPrecheckFailedEvent":
        return cls(
            workflow_id=data["workflow_id"],
            job=None,  # type: ignore
            job_id=data["job_id"],
            errors=data["errors"],
        )
