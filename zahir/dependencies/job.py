from collections.abc import Mapping
from typing import Any, Generic, TypedDict, TypeVar, cast

from zahir.base_types import Context, Dependency, DependencyState, JobRegistry, JobState

OutputType = TypeVar("OutputType", bound=Mapping)


class JobDependencyData(TypedDict, total=False):
    """Serialized structure for JobDependency."""

    job_id: str
    satisfied_states: list[str]
    impossible_states: list[str]


class JobDependency(Dependency, Generic[OutputType]):
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
        return DependencyState.UNSATISFIED

    def save(self) -> dict[str, Any]:
        """Save the job dependency to a dictionary."""

        return {
            "type": "JobDependency",
            "job_id": self.job_id,
            "satisfied_states": [state.value for state in self.satisfied_states],
            "impossible_states": [state.value for state in self.impossible_states],
        }

    @classmethod
    def load(cls, context: Context, data: Mapping[str, Any]) -> "JobDependency":
        """Load the job dependency from a dictionary."""

        job_id = data["job_id"]
        satisfied_states = {
            JobState(state) for state in data.get("satisfied_states", [])
        }
        impossible_states = {
            JobState(state) for state in data.get("impossible_states", [])
        }
        return cls(
            job_id=job_id,
            job_registry=context.job_registry,
            satisfied_states=satisfied_states if satisfied_states else None,
            impossible_states=impossible_states if impossible_states else None,
        )

    def output(self, context: Context) -> OutputType | None:
        """Get the output of the job, if available, from the registry"""
        return cast(OutputType | None, context.job_registry.get_output(self.job_id))

    def state(self) -> JobState:
        """Get the current state of the job from the registry"""
        return self.job_registry.get_state(self.job_id)
