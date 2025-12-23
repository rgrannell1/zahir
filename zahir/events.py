"""Events

Workflows should be observable. So we'll yield events describing the state of the workflow engine over time. The runner can dispatch these events as desired.
"""

from dataclasses import dataclass
from zahir.types import Job


class ZahirEvent:
    """Base class for all Zahir events"""

    pass


class WorkflowCompleteEvent(ZahirEvent):
    """Indicates that the workflow has completed"""

    pass


@dataclass
class JobRunnableEvent(ZahirEvent):
    """Indicates that a job is runnable"""

    job: "Job"


@dataclass
class JobCompletedEvent(ZahirEvent):
    """Indicates that a job has completed successfully"""

    job: "Job"


@dataclass
class JobStartedEvent(ZahirEvent):
    """Indicates that a job has started execution"""

    job: "Job"


@dataclass
class JobTimeoutEvent(ZahirEvent):
    """Indicates that a job has timed out"""

    job: "Job"


@dataclass
class JobRecoveryStarted(ZahirEvent):
    """Indicates that a job recovery has started"""

    job: "Job"


@dataclass
class JobRecoveryCompleted(ZahirEvent):
    """Indicates that a job recovery has completed"""

    job: "Job"


@dataclass
class JobRecoveryTimeout(ZahirEvent):
    """Indicates that a job recovery has timed out"""

    job: "Job"


@dataclass
class JobIrrecoverableEvent(ZahirEvent):
    """Indicates that a job recovery has failed irrecoverably"""

    error: Exception
    job: "Job"
