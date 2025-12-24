"""Events

Workflows should be observable. So we'll yield events describing the state of the workflow engine over time. The runner can dispatch these events as desired.
"""

from dataclasses import dataclass
from zahir.types import Job


class ZahirEvent:
    """Base class for all Zahir events"""

    pass


@dataclass
class WorkflowCompleteEvent(ZahirEvent):
    """Indicates that the workflow has completed"""

    workflow_id: str
    duration_seconds: float


@dataclass
class JobRunnableEvent(ZahirEvent):
    """Indicates that a job is runnable"""

    workflow_id: str
    job: "Job"
    job_id: str


@dataclass
class JobCompletedEvent(ZahirEvent):
    """Indicates that a job has completed successfully"""

    workflow_id: str
    job: "Job"
    job_id: str
    duration_seconds: float


@dataclass
class JobStartedEvent(ZahirEvent):
    """Indicates that a job has started execution"""

    workflow_id: str
    job: "Job"
    job_id: str


@dataclass
class JobTimeoutEvent(ZahirEvent):
    """Indicates that a job has timed out"""

    workflow_id: str
    job: "Job"
    job_id: str
    duration_seconds: float


@dataclass
class JobRecoveryStarted(ZahirEvent):
    """Indicates that a job recovery has started"""

    workflow_id: str
    job: "Job"
    job_id: str


@dataclass
class JobRecoveryCompleted(ZahirEvent):
    """Indicates that a job recovery has completed"""

    workflow_id: str
    job: "Job"
    job_id: str
    duration_seconds: float


@dataclass
class JobRecoveryTimeout(ZahirEvent):
    """Indicates that a job recovery has timed out"""

    workflow_id: str
    job: "Job"
    job_id: str


@dataclass
class JobIrrecoverableEvent(ZahirEvent):
    """Indicates that a job recovery has failed irrecoverably"""

    workflow_id: str
    error: Exception
    job: "Job"
    job_id: str


@dataclass
class JobPrecheckFailedEvent(ZahirEvent):
    """Indicates that a job's precheck validation failed"""

    workflow_id: str
    job: "Job"
    job_id: str
    errors: list[str]
