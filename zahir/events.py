"""Events

Workflows should be observable. So we'll yield events describing the state of the workflow engine over time. The runner can dispatch these events as desired.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, TypeVar

from zahir.serialisers.job import SerialisedJob

OutputType = TypeVar("OutputType", bound=Mapping[str, Any])
CustomEventOutputType = TypeVar("CustomEventOutputType", bound=Mapping[str, Any])


class ZahirEvent(ABC):
    """Base class for all Zahir events"""

    @abstractmethod
    def save(self) -> Mapping[str, Any]:
        """Serialize the event to a dictionary.

        @return: The serialized event data
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def load(cls, data: Mapping[str, Any]) -> ZahirEvent:
        """Deserialize the event from a dictionary.

        @param data: The serialized event data
        @return: The deserialized event
        """
        raise NotImplementedError

    def set_ids(self, job_id: str | None = None, workflow_id: str | None = None) -> None:
        """Set the job_id and workflow_id for the event if applicable.

        @param job_id: The job ID to set
        @param workflow_id: The workflow ID to set
        """
        if hasattr(self, "job_id") and job_id is not None:
            self.job_id = job_id

        if hasattr(self, "workflow_id") and workflow_id is not None:
            self.workflow_id = workflow_id


@dataclass
class WorkflowStartedEvent(ZahirEvent):
    """Indicates that the workflow has started"""

    workflow_id: str

    def save(self) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
        }

    @classmethod
    def load(cls, data: Mapping[str, Any]) -> WorkflowStartedEvent:
        return cls(
            workflow_id=data["workflow_id"],
        )


@dataclass
class WorkflowCompleteEvent(ZahirEvent):
    """Indicates that the workflow has completed"""

    workflow_id: str
    duration_seconds: float

    def save(self) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "duration_seconds": self.duration_seconds,
        }

    @classmethod
    def load(cls, data: Mapping[str, Any]) -> WorkflowCompleteEvent:
        return cls(workflow_id=data["workflow_id"], duration_seconds=data["duration_seconds"])


@dataclass
class WorkflowOutputEvent[OutputType](ZahirEvent):
    """Indicates that the workflow has produced output"""

    output: OutputType
    workflow_id: str | None = None
    job_id: str | None = None

    def __init__(
        self,
        output: OutputType,
        workflow_id: str | None = None,
        job_id: str | None = None,
    ) -> None:
        self.output = output
        self.workflow_id = workflow_id
        self.job_id = job_id

    def save(self) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "output": self.output,
            "job_id": self.job_id,
        }

    @classmethod
    def load(cls, data: Mapping[str, Any]) -> WorkflowOutputEvent:
        return WorkflowOutputEvent(
            workflow_id=data["workflow_id"],
            output=data["output"],
            job_id=data["job_id"],
        )


@dataclass
class JobCompletedEvent(ZahirEvent):
    """Indicates that a job has completed successfully"""

    workflow_id: str
    job_id: str
    duration_seconds: float

    def save(self) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "duration_seconds": self.duration_seconds,
        }

    @classmethod
    def load(cls, data: Mapping[str, Any]) -> JobCompletedEvent:
        return cls(
            workflow_id=data["workflow_id"],
            job_id=data["job_id"],
            duration_seconds=data["duration_seconds"],
        )


@dataclass
class JobOutputEvent[OutputType](ZahirEvent):
    """Indicates that a job has produced output"""

    output: OutputType
    workflow_id: str | None = None
    job_id: str | None = None

    def save(self) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "output": self.output,
        }

    @classmethod
    def load(cls, data: Mapping[str, Any]) -> JobOutputEvent:
        return JobOutputEvent(
            output=data["output"],
            workflow_id=data["workflow_id"],
            job_id=data["job_id"],
        )


@dataclass
class JobStartedEvent(ZahirEvent):
    """Indicates that a job has started execution"""

    workflow_id: str
    job_id: str

    def save(self) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
        }

    @classmethod
    def load(cls, data: Mapping[str, Any]) -> JobStartedEvent:
        return cls(
            workflow_id=data["workflow_id"],
            job_id=data["job_id"],
        )


@dataclass
class JobPausedEvent(ZahirEvent):
    """Indicates that a job has paused execution"""

    workflow_id: str
    job_id: str

    def save(self) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
        }

    @classmethod
    def load(cls, data: Mapping[str, Any]) -> JobPausedEvent:
        return cls(
            workflow_id=data["workflow_id"],
            job_id=data["job_id"],
        )


@dataclass
class JobTimeoutEvent(ZahirEvent):
    """Indicates that a job has timed out"""

    workflow_id: str
    job_id: str
    duration_seconds: float

    def save(self) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "duration_seconds": self.duration_seconds,
        }

    @classmethod
    def load(cls, data: Mapping[str, Any]) -> JobTimeoutEvent:
        return cls(
            workflow_id=data["workflow_id"],
            job_id=data["job_id"],
            duration_seconds=data["duration_seconds"],
        )


@dataclass
class JobRecoveryStartedEvent(ZahirEvent):
    """Indicates that a job recovery has started"""

    workflow_id: str
    job_id: str

    def save(self) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
        }

    @classmethod
    def load(cls, data: Mapping[str, Any]) -> JobRecoveryStartedEvent:
        return cls(
            workflow_id=data["workflow_id"],
            job_id=data["job_id"],
        )


@dataclass
class JobRecoveryCompletedEvent(ZahirEvent):
    """Indicates that a job recovery has completed"""

    workflow_id: str
    job_id: str
    duration_seconds: float

    def save(self) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "duration_seconds": self.duration_seconds,
        }

    @classmethod
    def load(cls, data: Mapping[str, Any]) -> JobRecoveryCompletedEvent:
        return cls(
            workflow_id=data["workflow_id"],
            job_id=data["job_id"],
            duration_seconds=data["duration_seconds"],
        )


@dataclass
class JobRecoveryTimeoutEvent(ZahirEvent):
    """Indicates that a job recovery has timed out"""

    workflow_id: str
    job_id: str
    duration_seconds: float

    def save(self) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "duration_seconds": self.duration_seconds,
        }

    @classmethod
    def load(cls, data: Mapping[str, Any]) -> JobRecoveryTimeoutEvent:
        return cls(
            workflow_id=data["workflow_id"],
            job_id=data["job_id"],
            duration_seconds=data["duration_seconds"],
        )


@dataclass
class JobIrrecoverableEvent(ZahirEvent):
    """Indicates that a job recovery has failed irrecoverably"""

    workflow_id: str
    error: BaseException
    job_id: str

    def save(self) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "error": str(self.error),
            "error_type": type(self.error).__name__,
        }

    @classmethod
    def load(cls, data: Mapping[str, Any]) -> JobIrrecoverableEvent:
        # Recreate a generic exception from the error string
        error_msg = data.get("error", "Unknown error")
        return cls(
            workflow_id=data["workflow_id"],
            error=Exception(error_msg),
            job_id=data["job_id"],
        )


@dataclass
class JobPrecheckFailedEvent(ZahirEvent):
    """Indicates that a job's precheck validation failed"""

    workflow_id: str
    job_id: str
    error: str

    def save(self) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "error": self.error,
        }

    @classmethod
    def load(cls, data: Mapping[str, Any]) -> JobPrecheckFailedEvent:
        return cls(
            workflow_id=data["workflow_id"],
            job_id=data["job_id"],
            error=data["error"],
        )


# Custom event for user-defined job outputs or signals
@dataclass
class ZahirCustomEvent[CustomEventOutputType](ZahirEvent):
    """Custom event for arbitrary job output or signals."""

    workflow_id: str | None = None
    job_id: str | None = None
    output: CustomEventOutputType | None = None

    def save(self) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "output": self.output,
        }

    @classmethod
    def load(cls, data: Mapping[str, Any]) -> ZahirCustomEvent:
        return cls(
            workflow_id=data.get("workflow_id"),
            output=data.get("output"),
        )


@dataclass
class ZahirInternalErrorEvent(ZahirEvent):
    """Indicates that an internal Zahir error has occurred"""

    workflow_id: str | None = None
    error: str | None = None

    def save(self) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "error": self.error,
        }

    @classmethod
    def load(cls, data: Mapping[str, Any]) -> ZahirInternalErrorEvent:
        return cls(
            workflow_id=data.get("workflow_id"),
            error=data.get("error"),
        )


@dataclass
class JobEvent(ZahirEvent):
    """Generic job event for various job state changes."""

    job: SerialisedJob

    def save(self) -> Mapping[str, Any]:
        return {
            "job": self.job,
        }

    @classmethod
    def load(cls, data: Mapping[str, Any]) -> JobEvent:
        return cls(
            job=data["job"],
        )


@dataclass
class Await(ZahirEvent):
    """Indicates that a job is awaiting some condition before proceeding"""

    job: Job | list[Job]

    def save(self) -> Mapping[str, Any]:
        from zahir.base_types.job import Job

        if isinstance(self.job, Job):
            return {
                "job": self.job.save(),
                "is_list": False,
            }
        return {
            "job": [j.save() for j in self.job],
            "is_list": True,
        }

    @classmethod
    def load(cls, data: Mapping[str, Any]) -> Await:

        # This requires context to deserialize jobs, which we don't have here
        # This is a design issue - load methods need context
        raise NotImplementedError("Await.load requires context to deserialize Job objects")
