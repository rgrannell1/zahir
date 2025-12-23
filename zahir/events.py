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
class JobRunnable(ZahirEvent):
    """Indicates that a job is runnable"""

    job: "Task"


@dataclass
class JobCompleted(ZahirEvent):
    """Indicates that a job has completed successfully"""

    job: "Task"
