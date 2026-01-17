"""Events

Workflows should be observable. So we'll yield events describing the state of the workflow engine over time. The runner can dispatch these events as desired.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, field
import os
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from zahir.base_types import Context, JobInstance, SerialisedJobInstance

OutputType = TypeVar("OutputType", bound=Mapping[str, Any])
CustomEventOutputType = TypeVar("CustomEventOutputType", bound=Mapping[str, Any])


class ZahirEvent(ABC):
    """Base class for all Zahir events"""

    @abstractmethod
    def save(self, context: Context) -> Mapping[str, Any]:
        """Serialize the event to a dictionary.

        @param context: The context for serialization
        @return: The serialized event data
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def load(cls, context: Context, data: Mapping[str, Any]) -> ZahirEvent:
        """Deserialize the event from a dictionary.

        @param context: The context for deserialization
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
    pid: int = field(default_factory=os.getpid)

    def save(self, context: Context) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "pid": self.pid,
        }

    @classmethod
    def load(cls, context: Context, data: Mapping[str, Any]) -> WorkflowStartedEvent:
        return cls(
            workflow_id=data["workflow_id"],
            pid=data.get("pid", 0),
        )


@dataclass
class WorkflowCompleteEvent(ZahirEvent):
    """Indicates that the workflow has completed"""

    workflow_id: str
    duration_seconds: float
    pid: int = field(default_factory=os.getpid)

    def save(self, context: Context) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "duration_seconds": self.duration_seconds,
            "pid": self.pid,
        }

    @classmethod
    def load(cls, context: Context, data: Mapping[str, Any]) -> WorkflowCompleteEvent:
        return cls(workflow_id=data["workflow_id"], duration_seconds=data["duration_seconds"], pid=data.get("pid", 0))


@dataclass
class WorkflowOutputEvent[OutputType](ZahirEvent):
    """Indicates that the workflow has produced output"""

    output: OutputType
    workflow_id: str | None = None
    job_id: str | None = None
    pid: int = field(default_factory=os.getpid)

    def __init__(
        self,
        output: OutputType,
        workflow_id: str | None = None,
        job_id: str | None = None,
        pid: int | None = None,
    ) -> None:
        self.output = output
        self.workflow_id = workflow_id
        self.job_id = job_id
        self.pid = pid if pid is not None else os.getpid()

    def save(self, context: Context) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "output": self.output,
            "job_id": self.job_id,
            "pid": self.pid,
        }

    @classmethod
    def load(cls, context: Context, data: Mapping[str, Any]) -> WorkflowOutputEvent:
        return WorkflowOutputEvent(
            workflow_id=data["workflow_id"],
            output=data["output"],
            job_id=data["job_id"],
            pid=data.get("pid", 0),
        )


@dataclass
class JobCompletedEvent(ZahirEvent):
    """Indicates that a job has completed successfully"""

    workflow_id: str
    job_id: str
    duration_seconds: float
    pid: int = field(default_factory=os.getpid)

    def save(self, context: Context) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "duration_seconds": self.duration_seconds,
            "pid": self.pid,
        }

    @classmethod
    def load(cls, context: Context, data: Mapping[str, Any]) -> JobCompletedEvent:
        return cls(
            workflow_id=data["workflow_id"],
            job_id=data["job_id"],
            duration_seconds=data["duration_seconds"],
            pid=data.get("pid", 0),
        )


@dataclass
class JobOutputEvent[OutputType](ZahirEvent):
    """Indicates that a job has produced output"""

    output: OutputType
    workflow_id: str | None = None
    job_id: str | None = None
    pid: int = field(default_factory=os.getpid)

    def save(self, context: Context) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "output": self.output,
            "pid": self.pid,
        }

    @classmethod
    def load(cls, context: Context, data: Mapping[str, Any]) -> JobOutputEvent:
        return JobOutputEvent(
            output=data["output"],
            workflow_id=data["workflow_id"],
            job_id=data["job_id"],
            pid=data.get("pid", 0),
        )


@dataclass
class JobStartedEvent(ZahirEvent):
    """Indicates that a job has started execution"""

    workflow_id: str
    job_id: str
    pid: int = field(default_factory=os.getpid)

    def save(self, context: Context) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "pid": self.pid,
        }

    @classmethod
    def load(cls, context: Context, data: Mapping[str, Any]) -> JobStartedEvent:
        return cls(
            workflow_id=data["workflow_id"],
            job_id=data["job_id"],
            pid=data.get("pid", 0),
        )


@dataclass
class JobPausedEvent(ZahirEvent):
    """Indicates that a job has paused execution"""

    workflow_id: str
    job_id: str
    pid: int = field(default_factory=os.getpid)

    def save(self, context: Context) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "pid": self.pid,
        }

    @classmethod
    def load(cls, context: Context, data: Mapping[str, Any]) -> JobPausedEvent:
        return cls(
            workflow_id=data["workflow_id"],
            job_id=data["job_id"],
            pid=data.get("pid", 0),
        )


@dataclass
class JobImpossibleEvent(ZahirEvent):
    """Indicates that a job has become impossible"""

    workflow_id: str
    job_id: str
    pid: int = field(default_factory=os.getpid)

    def save(self, context: Context) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "pid": self.pid,
        }

    @classmethod
    def load(cls, context: Context, data: Mapping[str, Any]) -> JobImpossibleEvent:
        return cls(
            workflow_id=data["workflow_id"],
            job_id=data["job_id"],
            pid=data.get("pid", 0),
        )


@dataclass
class JobTimeoutEvent(ZahirEvent):
    """Indicates that a job has timed out"""

    workflow_id: str
    job_id: str
    duration_seconds: float
    pid: int = field(default_factory=os.getpid)

    def save(self, context: Context) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "duration_seconds": self.duration_seconds,
            "pid": self.pid,
        }

    @classmethod
    def load(cls, context: Context, data: Mapping[str, Any]) -> JobTimeoutEvent:
        return cls(
            workflow_id=data["workflow_id"],
            job_id=data["job_id"],
            duration_seconds=data["duration_seconds"],
            pid=data.get("pid", 0),
        )


@dataclass
class JobRecoveryStartedEvent(ZahirEvent):
    """Indicates that a job recovery has started"""

    workflow_id: str
    job_id: str
    pid: int = field(default_factory=os.getpid)

    def save(self, context: Context) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "pid": self.pid,
        }

    @classmethod
    def load(cls, context: Context, data: Mapping[str, Any]) -> JobRecoveryStartedEvent:
        return cls(
            workflow_id=data["workflow_id"],
            job_id=data["job_id"],
            pid=data.get("pid", 0),
        )


@dataclass
class JobRecoveryCompletedEvent(ZahirEvent):
    """Indicates that a job recovery has completed"""

    workflow_id: str
    job_id: str
    duration_seconds: float
    pid: int = field(default_factory=os.getpid)

    def save(self, context: Context) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "duration_seconds": self.duration_seconds,
            "pid": self.pid,
        }

    @classmethod
    def load(cls, context: Context, data: Mapping[str, Any]) -> JobRecoveryCompletedEvent:
        return cls(
            workflow_id=data["workflow_id"],
            job_id=data["job_id"],
            duration_seconds=data["duration_seconds"],
            pid=data.get("pid", 0),
        )


@dataclass
class JobRecoveryTimeoutEvent(ZahirEvent):
    """Indicates that a job recovery has timed out"""

    workflow_id: str
    job_id: str
    duration_seconds: float
    pid: int = field(default_factory=os.getpid)

    def save(self, context: Context) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "duration_seconds": self.duration_seconds,
            "pid": self.pid,
        }

    @classmethod
    def load(cls, context: Context, data: Mapping[str, Any]) -> JobRecoveryTimeoutEvent:
        return cls(
            workflow_id=data["workflow_id"],
            job_id=data["job_id"],
            duration_seconds=data["duration_seconds"],
            pid=data.get("pid", 0),
        )


@dataclass
class JobIrrecoverableEvent(ZahirEvent):
    """Indicates that a job recovery has failed irrecoverably"""

    workflow_id: str
    error: Exception
    job_id: str
    pid: int = field(default_factory=os.getpid)

    def save(self, context: Context) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "error": str(self.error),
            "error_type": type(self.error).__name__,
            "pid": self.pid,
        }

    @classmethod
    def load(cls, context: Context, data: Mapping[str, Any]) -> JobIrrecoverableEvent:
        # Recreate a generic exception from the error string
        error_msg = data.get("error", "Unknown error")
        return cls(
            workflow_id=data["workflow_id"],
            error=Exception(error_msg),
            job_id=data["job_id"],
            pid=data.get("pid", 0),
        )


@dataclass
class JobPrecheckFailedEvent(ZahirEvent):
    """Indicates that a job's precheck validation failed"""

    workflow_id: str
    job_id: str
    error: str
    pid: int = field(default_factory=os.getpid)

    def save(self, context: Context) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "error": self.error,
            "pid": self.pid,
        }

    @classmethod
    def load(cls, context: Context, data: Mapping[str, Any]) -> JobPrecheckFailedEvent:
        return cls(
            workflow_id=data["workflow_id"],
            job_id=data["job_id"],
            error=data["error"],
            pid=data.get("pid", 0),
        )


# Custom event for user-defined job outputs or signals
@dataclass
class ZahirCustomEvent[CustomEventOutputType](ZahirEvent):
    """Custom event for arbitrary job output or signals."""

    workflow_id: str | None = None
    job_id: str | None = None
    output: CustomEventOutputType | None = None
    pid: int = field(default_factory=os.getpid)

    def save(self, context: Context) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "output": self.output,
            "pid": self.pid,
        }

    @classmethod
    def load(cls, context: Context, data: Mapping[str, Any]) -> ZahirCustomEvent:
        return cls(
            workflow_id=data.get("workflow_id"),
            job_id=data.get("job_id"),
            output=data.get("output"),
            pid=data.get("pid", 0),
        )


@dataclass
class ZahirInternalErrorEvent(ZahirEvent):
    """Indicates that an internal Zahir error has occurred"""

    workflow_id: str | None = None
    error: str | None = None
    pid: int = field(default_factory=os.getpid)

    def save(self, context: Context) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "error": self.error,
            "pid": self.pid,
        }

    @classmethod
    def load(cls, context: Context, data: Mapping[str, Any]) -> ZahirInternalErrorEvent:
        return cls(
            workflow_id=data.get("workflow_id"),
            error=data.get("error"),
            pid=data.get("pid", 0),
        )


@dataclass
class JobEvent(ZahirEvent):
    """Generic job event for various job state changes."""

    job: SerialisedJobInstance
    pid: int = field(default_factory=os.getpid)

    def save(self, context: Context) -> Mapping[str, Any]:
        return {
            "job": self.job,
            "pid": self.pid,
        }

    @classmethod
    def load(cls, context: Context, data: Mapping[str, Any]) -> JobEvent:
        return cls(
            job=data["job"],
            pid=data.get("pid", 0),
        )


@dataclass
@dataclass
class Await(ZahirEvent):
    """Indicates that a job is awaiting some condition before proceeding"""

    job: JobInstance | list[JobInstance]
    pid: int = field(default_factory=os.getpid)

    def save(self, context: Context) -> Mapping[str, Any]:
        from zahir.base_types import JobInstance

        if isinstance(self.job, JobInstance):
            return {
                "job": self.job.save(context),
                "is_list": False,
                "pid": self.pid,
            }
        return {
            "job": [j.save(context) for j in self.job],
            "is_list": True,
            "pid": self.pid,
        }

    @classmethod
    def load(cls, context: Context, data: Mapping[str, Any]) -> Await:
        from zahir.base_types import JobInstance

        if data.get("is_list"):
            jobs = [JobInstance.load(context, j) for j in data["job"]]
            return cls(job=jobs)
        job = JobInstance.load(context, data["job"])
        return cls(job=job)


@dataclass
class JobWorkerWaitingEvent(ZahirEvent):
    """Indicates that a job worker is waiting for jobs to process"""

    pid: int = field(default_factory=os.getpid)

    def save(self, context: Context) -> Mapping[str, Any]:
        return {
            "pid": self.pid,
        }

    @classmethod
    def load(cls, context: Context, data: Mapping[str, Any]) -> JobWorkerWaitingEvent:
        return cls(pid=data.get("pid", 0))


@dataclass
class JobReadyEvent(ZahirEvent):
    """Indicates that a job is ready to be processed. Use the
    database to fetch the job details."""

    pid: int = field(default_factory=os.getpid)

    def save(self, context: Context) -> Mapping[str, Any]:
        return {
            "pid": self.pid,
        }

    @classmethod
    def load(cls, context: Context, data: Mapping[str, Any]) -> JobReadyEvent:
        return cls(pid=data.get("pid", 0))


@dataclass
class JobAssignedEvent(ZahirEvent):
    """Indicates that a job has been assigned to a worker"""

    workflow_id: str
    job_id: str
    pid: int = field(default_factory=os.getpid)

    def save(self, context: Context) -> Mapping[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "job_id": self.job_id,
            "pid": self.pid,
        }

    @classmethod
    def load(cls, context: Context, data: Mapping[str, Any]) -> JobAssignedEvent:
        return cls(
            workflow_id=data["workflow_id"],
            job_id=data["job_id"],
            pid=data.get("pid", 0),
        )
