
from zahir.types import Dependency, DependencyState, JobRegistry, JobState


class JobDependency(Dependency):
    """A dependency on another job's completion."""

    job_id: str
    job_registry: JobRegistry

    def __init__(
        self,
        job_id: str,
        job_registry: JobRegistry,
        states: set["JobState"] | None = None,
    ) -> None:
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

    def save(self) -> dict:
        """Save the job dependency to a dictionary."""

        return {
            "job_id": self.job_id,
            "states": [state.value for state in self.states],
        }

    @classmethod
    def load(cls, context, data: dict) -> "JobDependency":
        """Load the job dependency from a dictionary."""

        job_id = data["job_id"]
        states = {JobState(state) for state in data["states"]}
        return cls(job_id=job_id, job_registry=context.job_registry, states=states)
