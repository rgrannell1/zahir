import sys

from zahir.base_types import (
    Context,
    EventRegistry,
    JobInformation,
    JobRegistry,
    JobState,
)


class ZahirLogger:
    def __init__(self, job_registry: "JobRegistry") -> None:
        self.job_registry = job_registry
