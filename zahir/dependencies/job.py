
from enum import Enum

from zahir.types import Dependency, DependencyState, JobRegistry, JobState


class JobDependency(Dependency):
    """A dependency on another job's completion."""

    job_id: int
    job_registry: "JobRegistry"

    def __init__(
            self,
            job_id: int,
            job_registry: "JobRegistry",
            states: set["JobState"] | None = None) -> None:

        if states is None:
            self.states = {JobState.COMPLETED}
        else:
            self.states = states

        self.job_id = job_id
        self.job_registry = job_registry

    def satisfied(self) -> DependencyState:
        """Check whether the job dependency is satisfied."""

        state = self.job_registry.get_state(self.job_id)
        if state in self.states:
            return DependencyState.SATISFIED
        else:
            return DependencyState.UNSATISFIED
