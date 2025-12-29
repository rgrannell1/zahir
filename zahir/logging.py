from zahir.base_types import (
    JobRegistry,
)


class ZahirLogger:
    def __init__(self, job_registry: "JobRegistry") -> None:
        self.job_registry = job_registry
