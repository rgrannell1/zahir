"""Events

Workflows should be observable. So we'll yield events describing the state of the workflow engine over time. The runner can dispatch these events as desired.
"""

from dataclasses import dataclass
from zahir.types import Task


class ZahirEvent:
    """Base class for all Zahir events"""

    pass


class WorkflowCompleteEvent(ZahirEvent):
    """Indicates that the workflow has completed"""

    pass


@dataclass
class JobRunnableEvent(ZahirEvent):
    """Indicates that a job is runnable"""

    job: "Task"


@dataclass
class JobCompletedEvent(ZahirEvent):
    """Indicates that a job has completed successfully"""

    job: "Task"


@dataclass
class JobStartedEvent(ZahirEvent):
    """Indicates that a job has started execution"""

    job: "Task"


@dataclass
class JobTimeoutEvent(ZahirEvent):
    """Indicates that a job has timed out"""

    job: "Task"


@dataclass
class JobRecoveryStarted(ZahirEvent):
    """Indicates that a job recovery has started"""

    job: "Task"


@dataclass
class JobRecoveryCompleted(ZahirEvent):
    """Indicates that a job recovery has completed"""

    job: "Task"


@dataclass
class JobRecoveryTimeout(ZahirEvent):
    """Indicates that a job recovery has timed out"""

    job: "Task"


@dataclass
class JobIrrecoverableEvent(ZahirEvent):
    """Indicates that a job recovery has failed irrecoverably"""

    error: Exception
    job: "Task"
