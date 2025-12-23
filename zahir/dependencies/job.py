
from zahir.types import Dependency, DependencyState, JobRegistry

class JobDependency(Dependency):
    """A dependency on another job's completion."""

    job_id: int
    job_registry: "JobRegistry"

    def __init__(self, job_id: int, job_registry: "JobRegistry") -> None:
        self.job_id = job_id
        self.job_registry = job_registry

    def satisfied(self) -> DependencyState:
        """Check whether the job dependency is satisfied."""

        raise NotImplementedError
