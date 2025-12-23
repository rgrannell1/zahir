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

    duration_seconds: float


@dataclass
class JobRunnableEvent(ZahirEvent):
    """Indicates that a job is runnable"""

    job: "Job"
    job_id: int


@dataclass
class JobCompletedEvent(ZahirEvent):
    """Indicates that a job has completed successfully"""

    job: "Job"
    job_id: int
    duration_seconds: float


@dataclass
class JobStartedEvent(ZahirEvent):
    """Indicates that a job has started execution"""

    job: "Job"
    job_id: int


@dataclass
class JobTimeoutEvent(ZahirEvent):
    """Indicates that a job has timed out"""

    job: "Job"
    job_id: int
    duration_seconds: float


@dataclass
class JobRecoveryStarted(ZahirEvent):
    """Indicates that a job recovery has started"""

    job: "Job"
    job_id: int


@dataclass
class JobRecoveryCompleted(ZahirEvent):
    """Indicates that a job recovery has completed"""

    job: "Job"
    job_id: int
    duration_seconds: float


@dataclass
class JobRecoveryTimeout(ZahirEvent):
    """Indicates that a job recovery has timed out"""

    job: "Job"
    job_id: int


@dataclass
class JobIrrecoverableEvent(ZahirEvent):
    """Indicates that a job recovery has failed irrecoverably"""

    error: Exception
    job: "Job"
    job_id: int
