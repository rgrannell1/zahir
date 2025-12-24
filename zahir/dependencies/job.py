
from zahir.types import Dependency, DependencyState, JobRegistry, JobState


class JobDependency(Dependency):
    """A dependency on another job's completion."""

    job_id: str
    job_registry: JobRegistry

    def __init__(
        self,
        job_id: str,
        job_registry: JobRegistry,
        satisfied_states: set["JobState"] | None = None,
        impossible_states: set["JobState"] | None = None,
    ) -> None:
        if satisfied_states is None:
            self.satisfied_states = {JobState.COMPLETED}
        else:
            self.satisfied_states = satisfied_states

        if impossible_states is None:
            self.impossible_states = {JobState.IRRECOVERABLE, JobState.IMPOSSIBLE}
        else:
            self.impossible_states = impossible_states

        self.job_id = job_id
        self.job_registry = job_registry

    def satisfied(self) -> DependencyState:
        """Check whether the job dependency is satisfied."""

        state = self.job_registry.get_state(self.job_id)
        if state in self.impossible_states:
            return DependencyState.IMPOSSIBLE

        if state in self.satisfied_states:
            return DependencyState.SATISFIED
        else:
            return DependencyState.UNSATISFIED

    def save(self) -> dict:
        """Save the job dependency to a dictionary."""

        return {
            "job_id": self.job_id,
            "satisfied_states": [state.value for state in self.satisfied_states],
            "impossible_states": [state.value for state in self.impossible_states],
        }

    @classmethod
    def load(cls, context, data: dict) -> "JobDependency":
        """Load the job dependency from a dictionary."""

        job_id = data["job_id"]
        satisfied_states = {JobState(state) for state in data.get("satisfied_states", [])}
        impossible_states = {JobState(state) for state in data.get("impossible_states", [])}
        return cls(
            job_id=job_id,
            job_registry=context.job_registry,
            satisfied_states=satisfied_states if satisfied_states else None,
            impossible_states=impossible_states if impossible_states else None,
        )
