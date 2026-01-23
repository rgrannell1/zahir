"""Core type definitions used throughout Zahir."""

from abc import ABC, abstractmethod
from collections.abc import Generator, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
import multiprocessing
from multiprocessing.managers import DictProxy, SyncManager
from multiprocessing.queues import Queue as MPQueue
from zahir.exception import JobPrecheckError
from zahir.exception import JobPostcheckError

from typeguard import check_type
from typing import TYPE_CHECKING, Any, Self, TypedDict, TypeVar, cast

from zahir.events import Await, ZahirEvent
from zahir.utils.id_generator import generate_job_id

if TYPE_CHECKING:
    from zahir.dependencies.group import DependencyGroup
    from zahir.events import JobOutputEvent, WorkflowOutputEvent, ZahirCustomEvent

# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++ Dependency ++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


class DependencyState(StrEnum):
    """The state of a dependency."""

    SATISFIED = "satisfied"
    UNSATISFIED = "unsatisfied"
    IMPOSSIBLE = "impossible"


class DependencyData(TypedDict, total=False):
    """Base structure for serialized dependency data.

    Specific dependency types may add additional fields.
    """

    type: str


class Dependency(ABC):
    """A dependency, on which a job can depend"""

    @abstractmethod
    def satisfied(self) -> DependencyState:
        """Is the dependency satisfied?"""

        raise NotImplementedError

    @abstractmethod
    def request_extension(self, extra_seconds: float) -> Self:
        """A dependency may, if it chooses, allow you to request that it's valid longer
        than initially defined. This applies to time-based dependencies; other dependencies
        can just return themselves unchanged. Dependencies do not have to honour extension
        requests (sometimes we want hard-stops).

        The main use of these extensions is to support retries and backoff for jobs that use
        time-dependencies for scheduling.
        """
        ...

    @abstractmethod
    def save(self, context: "Context") -> Mapping[str, Any]:
        """Serialize the dependency to a dictionary.

        @param context: The context containing scope and registries
        @return: The serialized dependency data
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def load(cls, context: "Context", data: Mapping[str, Any]) -> Self:
        raise NotImplementedError


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++ Job Registry +++++++++++++++++++++++++++++++
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


class JobState(StrEnum):
    """Track the state jobs can be in"""

    # Still to be run
    PENDING = "pending"

    # Dependencies have not yet been satisfied for this job
    BLOCKED = "blocked"

    # Dependencies can never be satisfied, so
    # this job is impossible to run
    IMPOSSIBLE = "impossible"

    # Ready to run
    READY = "ready"

    # Claimed, now paused
    PAUSED = "paused"

    # Input validations for the job failed
    PRECHECK_FAILED = "precheck_failed"

    # Currently running
    RUNNING = "running"

    # Completed successfully
    COMPLETED = "completed"

    # Recovery running
    RECOVERING = "recovering"

    # Recovered successfully
    RECOVERED = "recovered"

    # Execution timed out
    TIMED_OUT = "timed_out"

    # Recovery timed out
    RECOVERY_TIMED_OUT = "recovery_timed_out"

    # Even rollback failed; this job is irrecoverable
    IRRECOVERABLE = "irrecoverable"


# Terminal job-states
COMPLETED_JOB_STATES = {
    JobState.IMPOSSIBLE,
    JobState.PRECHECK_FAILED,
    JobState.COMPLETED,
    JobState.RECOVERED,
    JobState.TIMED_OUT,
    JobState.RECOVERY_TIMED_OUT,
    JobState.IRRECOVERABLE,
}


# Active, non-terminal job-states
ACTIVE_JOB_STATES = {
    JobState.PENDING,
    JobState.BLOCKED,
    JobState.READY,
    JobState.PAUSED,
    JobState.RECOVERING,
    JobState.RUNNING,
}


def check_job_states_coverage() -> None:
    """Ensure that active and completed job states cover all possible job states."""

    # Verify that active and completed states cover all possible states
    total_job_states = set(JobState)

    covered_states = ACTIVE_JOB_STATES.union(COMPLETED_JOB_STATES)
    if covered_states != total_job_states:
        missing_states = total_job_states - covered_states
        raise ValueError(f"Job states coverage is incomplete. Missing states: {missing_states}")


check_job_states_coverage()


@dataclass
class JobInformation:
    """Holds information about a job stored in the job registry."""

    # The job's ID
    job_id: str
    # The job instance
    job: "JobInstance"
    # The job's current state
    state: JobState
    # The output of the job, if there is one
    output: Mapping[str, Any] | None

    # When did the job start?
    started_at: datetime | None = None
    # When did the job complete?
    completed_at: datetime | None = None
    # The workflow ID this job belongs to
    workflow_id: str | None = None


@dataclass
class JobTimingInformation:
    """Timing information for a job."""

    started_at: datetime | None
    recovery_started_at: datetime | None
    completed_at: datetime | None

    def time_since_started(self) -> float | None:
        """Get the time since the job started in seconds."""

        if self.started_at is None:
            return None

        now = datetime.now(tz=UTC)
        delta = now - self.started_at
        return delta.total_seconds()

    def time_since_recovery_started(self) -> float | None:
        """Get the time since the job recovery started in seconds."""

        if self.recovery_started_at is None:
            return None

        now = datetime.now(tz=UTC)
        delta = now - self.recovery_started_at
        return delta.total_seconds()


class JobRegistry(ABC):
    """Keeps track of jobs to be run."""

    @abstractmethod
    def init(self, worker_id: str) -> None:
        """Initialize the job registry for use by a particular worker.

        @param worker_id: The ID of the worker using this registry
        """

        raise NotImplementedError

    @abstractmethod
    def add(self, context: "Context", job: "JobInstance", output_queue, workflow_id: str) -> str:
        """
        Register a job with the job registry, returning a job ID.

        This method MUST ensure that each job_id is registered at most once:
        - If a job with the same job_id already exists in the registry, this method MUST raise an exception.
        - This ensures that each job is run at most once per registry instance.

        @param job: The job to register
        @param workflow_id: The ID of the workflow this job belongs to
        @return: The job ID assigned to the job
        @raises ValueError: If the job_id is already registered
        """

        raise NotImplementedError

    @abstractmethod
    def is_active(self, workflow_id: str | None = None) -> bool:
        """Return True if any jobs are in a non-terminal state

        @param workflow_id: Optional workflow ID to filter by. If None, checks all workflows.
        @return: True if there are any active jobs (optionally for the specified workflow)
        """

        raise NotImplementedError

    @abstractmethod
    def is_finished(self, job_id: str) -> bool:
        """Is the job in a terminal state?"""

        raise NotImplementedError

    @abstractmethod
    def get_state(self, job_id: str) -> JobState:
        """Get the state of a job by ID"""

        raise NotImplementedError

    @abstractmethod
    def get_job_timing(self, job_id: str) -> "JobTimingInformation":
        """Get timing information for a job by ID"""

        raise NotImplementedError

    @abstractmethod
    def set_state(
        self,
        context: "Context",
        job_id: str,
        job_type: str,
        workflow_id: str,
        output_queue: multiprocessing.Queue,
        state: JobState,
        recovery: bool = False,
        error: Exception | None = None,
        pid: int | None = None,
    ) -> str:
        """Set the state of a job by ID.

        @param pid: Optional process ID of the worker executing the job. Used for events like JobStartedEvent.
        """

        raise NotImplementedError

    @abstractmethod
    def get_output(self, job_id: str, recovery: bool = False) -> Mapping | None:
        """Retrieve the output of a completed job

        @param job_id: The ID of the job
        @return: The output dictionary, or None if no output was set
        """

        raise NotImplementedError

    @abstractmethod
    def set_output(
        self,
        context: "Context",
        job_id: str,
        job_type: str,
        workflow_id: str,
        output_queue,
        output: Mapping,
        recovery: bool = False,
    ) -> None:
        """Store the output of a completed job

        @param job_id: The ID of the job
        @param job_type: The type of the job
        @param output: The output dictionary produced by the job
        @param recovery: Whether this output is from a recovery job
        """

        raise NotImplementedError

    @abstractmethod
    def get_errors(self, job_id: str, recovery: bool = False) -> list[Exception]:
        """Retrieve the errors associated with a job. A job can have multiple
        errors potentially (precheck errror, then recovery error).

        @param job_id: The ID of the job
        @return: A list of errors
        """

        raise NotImplementedError

    @abstractmethod
    def get_active_workflow_ids(self) -> list[str]:
        """Get a list of all workflow IDs that have active jobs.

        @return: List of workflow IDs with active jobs
        """
        raise NotImplementedError

    @abstractmethod
    def jobs(
        self, context: "Context", state: JobState | None = None, workflow_id: str | None = None
    ) -> Iterator[JobInformation]:
        """Get an iterator of all jobs with their information.

        @param context: The context containing scope and registries for deserialization
        @param state: Optional state to filter jobs by
        @param workflow_id: Optional workflow ID to filter jobs by
        @return: An iterator of JobInformation objects
        """

        raise NotImplementedError

    @abstractmethod
    def set_claim(self, job_id: str, worker_id: str) -> bool:
        """Set a claim for a job by a worker.

        Used by the overseer to mark a job as being processed by a specific worker.

        @param job_id: The ID of the job to claim
        @param worker_id: The ID of the worker claiming the job
        @return: True if the claim was set successfully
        """

        raise NotImplementedError

    @abstractmethod
    def get_workflow_duration(self, workflow_id: str | None = None) -> float | None:
        """Get the duration of the workflow in seconds.

        @param workflow_id: Optional workflow ID to filter by. If None, computes duration for all workflows.
        @return: The duration in seconds, or None if workflow hasn't completed
        """

        raise NotImplementedError

    @abstractmethod
    def on_startup(self) -> None:
        """Hook called when the job registry is started up."""

        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        """Close the job registry."""

        raise NotImplementedError


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++ Event Registry ++++++++++++++++++++++++++++
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


class EventRegistry(ABC):
    """Keeps track of events occurring during workflow execution."""

    @abstractmethod
    def register(self, event: "ZahirEvent") -> None:
        """Register an event in the event registry."""

        raise NotImplementedError


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++ Job +++++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

ArgsType = TypeVar("ArgsType", bound=dict)
OutputType = TypeVar("OutputType", bound=dict)


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++ Scope +++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


class Scope(ABC):
    """We persist serialised dependency and job information to
    various stores. These ultimately need to be deserialized back into
    their correct Python classes.

    So, we'll explicitly register jobs and dependencies with a scope, to
    aid with this translation. The alternatives is decorators registering
    tasks at import-time, which is too side-effectful. Explicit > implicit.
    """

    @abstractmethod
    def get_job_spec(self, type_name: str) -> "JobSpec": ...

    @abstractmethod
    def add_job_specs(self, specs: list["JobSpec"]) -> Self:
        """Add multiple job specs to the scope."""
        ...

    @abstractmethod
    def add_job_spec(self, spec: "JobSpec") -> Self:
        """Add a job spec"""
        ...

    @abstractmethod
    def add_dependency_class(self, dependency_class: type[Dependency]) -> Self:
        """Add a dependency class to the scope."""
        ...

    @abstractmethod
    def add_dependency_classes(self, dependency_classes: list[type[Dependency]]) -> Self:
        """Add multiple dependency classes to the scope."""
        ...

    @abstractmethod
    def get_dependency_class(self, type_name: str) -> type[Dependency]:
        """Get a dependency class by its type name."""
        ...

    @abstractmethod
    def get_transform(self, type_name: str) -> "Transform":
        """Get a transform function by its type name."""
        ...

    @abstractmethod
    def add_transform(self, type_name: str, transform: "Transform") -> Self:
        """Add a transform function to the scope."""
        ...


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++ Context +++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


@dataclass
class Context:
    """Context such as scope, the job registry, and event registry
    needs to be communicated to Jobs and Dependencies.
    """

    # The scope containing registered job and dependency classes
    scope: Scope
    # Keep track of jobs
    job_registry: JobRegistry
    # Shared state creator
    manager: SyncManager
    # Shared state dictionary. Used for storing semaphores (concurrencylimit)
    state: DictProxy[str, Any]

    @abstractmethod
    def get_queue(self, name: str) -> MPQueue[Any]:
        """Get a named queue from the context."""

        raise NotImplementedError


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# ++++++++++++++++++++++ Jobs, Take II +++++++++++++++++++++++++++++
#
# Jobs conceptually have a few parts.
#
# The JobSpec defines how to run, recover, and precheck a job type. This
#   can't be serialised, but we use a `type` label as a label and `Scope`
#   as a way of getting the JobSpec back.
#
# We would like to transform jobs
#   e.g this job, but retry five times, or this job with exponential backoff.
#   We don't want to pre-store all cross-products of transforms and jobs in scope.
#   We treat transforms as f:JobSpec -> JobSpec functions. We store which transforms we
#   applied to a job (e.g `[[retry, args] `type`]`) to reconstruct the job-instances
#   correctly later.
#
# Job-instances are JobSpec x { Input, Dependencies, Options, Metadata }
#
# The latter part is more easily serialised.
#
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

from collections.abc import Callable, Mapping

type JobEventSet[OutputType] = (
    # Jobs can output jobs
    JobInstance
    |
    # ... and output
    JobOutputEvent[OutputType]
    |
    # ... and workflow output (streamed)
    WorkflowOutputEvent
    |
    # ... and something custom
    ZahirCustomEvent
    |
    # ... or await other jobs
    Await
)

type Precheck[ArgsType] = Callable[[ArgsType], Exception | None]

type Run[ArgsType, JobOutputType] = Callable[
    [Context, ArgsType, DependencyGroup],
    Generator[JobEventSet[JobOutputType], Any, Any],
]

type Recover[ArgsType, JobOutputType] = Callable[
    [Context, ArgsType, DependencyGroup, Exception],
    Generator[JobEventSet[JobOutputType], Any, Any],
]


def default_recover[ArgsType](
    context: Context,
    args: ArgsType,  # type: ignore
    dependencies: "DependencyGroup",
    err: Exception,
) -> Generator[JobEventSet]:
    """Default recovery function that simply re-raises the original exception."""

    raise err
    yield


def create_typeddict_precheck[ArgsType](
    args_type: type[ArgsType] | None,
) -> Precheck[ArgsType]:
    """Create a precheck function that validates against a TypedDict using typeguard."""

    if args_type is None:
        return lambda args: None

    def typeddict_precheck(args: ArgsType) -> Exception | None:
        try:
            check_type(args, args_type)
            return None
        except Exception as err:
            exc = JobPrecheckError(f"TypedDict validation failed")
            exc.__cause__ = err
            return exc

    return typeddict_precheck


def validate_output_type(output: Any, output_type: type[Any] | None) -> Exception | None:
    """Validate that output matches the TypedDict structure using typeguard.

    @param output: The output to validate
    @param output_type: The TypedDict class to validate against, or None
    @return: None if valid, Exception if invalid
    """
    if output_type is None:
        return None

    try:
        check_type(output, output_type)
        return None
    except Exception as err:
        exc = JobPostcheckError(f"TypedDict output validation failed")
        exc.__cause__ = err
        return exc


class SerialisedTransformSpec(TypedDict):
    """Serialized form of a transform specification."""

    type: str
    args: Mapping[str, Any]


class SerialisedJobData(TypedDict):
    type: str
    transforms: list[SerialisedTransformSpec]


@dataclass
class TransformSpec:
    """A specification for a transform to apply to a JobSpec.

    Transforms are looked up by type in the scope, then applied with the given args
    to produce a transformed JobSpec.
    """

    # The type label for this transform (used for scope lookup)
    type: str

    # Arguments to pass to the transform function
    args: Mapping[str, Any] = field(default_factory=dict)

    def save(self) -> SerialisedTransformSpec:
        """Serialize the transform spec to a dictionary."""
        return {
            "type": self.type,
            "args": dict(self.args),
        }

    @classmethod
    def load(cls, data: SerialisedTransformSpec) -> "TransformSpec":
        """Load a transform spec from serialized data."""
        return cls(type=data["type"], args=data.get("args", {}))


@dataclass
class JobSpec[ArgsType, OutputType]:
    """The specification for a job; how it runs, precovers, and prechecks. Jobspecs
    are later associated with job-parameters to form an actual job."""

    # a label uniquely associated with this job-spec
    type: str

    # run the job
    run: Run[ArgsType, OutputType]

    # a list of transformations that must be applied to the functions before they
    # can be run. Each transform spec contains a type label and arguments dict.
    transforms: list[TransformSpec] = field(default_factory=list)

    # recover on uncaught exception
    recover: Recover[ArgsType, OutputType] | None = default_recover

    # precheck inputs
    precheck: Precheck[ArgsType] | None = field(default=None)

    # Optional TypedDict classes for runtime input/output validation
    args_type: type[ArgsType] | None = None
    output_type: type[OutputType] | None = None

    def __post_init__(self):
        """Set up default precheck if args_type is provided and no custom precheck."""
        if self.precheck is None:
            self.precheck = create_typeddict_precheck(self.args_type)

    def save(self) -> SerialisedJobData:
        """Serialize the job spec to a dictionary.

        @return: The serialized job spec
        """

        return {
            "type": self.type,
            "transforms": [transform.save() for transform in self.transforms],
        }

    def with_transform(
        self, transform_type: str, args: Mapping[str, Any] | None = None
    ) -> "JobSpec[ArgsType, OutputType]":
        """Create a new JobSpec with an additional transform appended.

        @param transform_type: The type label for the transform (looked up in scope)
        @param args: Arguments to pass to the transform function
        @return: A new JobSpec with the transform added
        """
        new_transforms = self.transforms.copy()
        new_transforms.append(TransformSpec(type=transform_type, args=args or {}))

        return JobSpec(
            type=self.type,
            run=self.run,
            transforms=new_transforms,
            recover=self.recover,
            precheck=self.precheck,
            args_type=self.args_type,
            output_type=self.output_type,
        )

    def apply_transforms(self, scope: "Scope") -> "JobSpec[ArgsType, OutputType]":
        """Apply all transforms to produce the final runnable JobSpec.

        Transforms are applied in order, each one wrapping the previous result.
        The original transforms list is preserved on the result so it can be
        serialized and re-applied after loading.

        @param scope: The scope containing registered transforms
        @return: The transformed JobSpec ready for execution
        """

        # If no transforms, return the original spec
        if not self.transforms:
            return self

        # Start with the base spec
        result: JobSpec = JobSpec(
            type=self.type,
            run=self.run,
            transforms=self.transforms,  # Preserve transforms for serialization
            recover=self.recover,
            precheck=self.precheck,
            args_type=self.args_type,
            output_type=self.output_type,
        )

        # ...then, apply each transform in order
        for transform_spec in self.transforms:
            transform_fn = scope.get_transform(transform_spec.type)
            result = transform_fn(transform_spec.args, result)

            # Keep the transforms list on the result for serialization
            # So this can be replayed on load
            result = JobSpec(
                type=result.type,
                run=result.run,
                transforms=self.transforms,
                recover=result.recover,
                precheck=result.precheck,
                args_type=result.args_type,
                output_type=result.output_type,
            )

        return result

    def __call__(
        self,
        args: ArgsType,
        dependencies: "Mapping[str, Dependency] | DependencyGroup | None" = None,
        job_timeout: float | None = None,
        recover_timeout: float | None = None,
        job_id: str | None = None,
        priority: int = 0,
        once: bool = False,
        once_by: "Callable[[str, Mapping[str, Any], Mapping[str, Any]], str] | None" = None,
    ) -> "JobInstance[ArgsType, OutputType]":
        if job_id is None:
            job_id = generate_job_id(self.type)

        from zahir.dependencies.group import DependencyGroup

        processed_dependencies = DependencyGroup({})
        if isinstance(dependencies, DependencyGroup):
            processed_dependencies = dependencies
        elif isinstance(dependencies, dict):
            processed_dependencies = DependencyGroup(dependencies)

        job_args = JobArguments[ArgsType](
            dependencies=processed_dependencies,
            args=args,
            job_timeout=job_timeout,
            recover_timeout=recover_timeout,
            job_id=job_id,
            # We'll set this at the workflow level.
            parent_id=None,
            priority=priority,
            once=once,
            once_by=once_by,
        )

        return JobInstance(spec=self, args=job_args)


@dataclass
class JobArguments[ArgsType]:
    """The things provided along with a JobSpec to form a job instance"""

    # The group of dependencies that determine under what condition we'll run this job
    dependencies: "DependencyGroup"

    # The actual arguments to the job
    args: ArgsType

    # The unique identifier for this job instance
    job_id: str

    # Optional parent job ID for traceability;
    # only optional in the root job
    parent_id: str | None = None

    # Upper-limit on how long the job should run for
    job_timeout: float | None = None

    # Upper-limit on how long the recovery should run for
    recover_timeout: float | None = None

    # Priority for job scheduling (higher values run first)
    priority: int = 0

    # If True, only add this job once based on an idempotence key
    once: bool = False

    # Compute a hash for idempotence checking, based on deps + inputs
    once_by: "Callable[[str, Mapping[str, Any], Mapping[str, Any]], str] | None" = None


class SerialisedJobInstance[ArgsType](TypedDict, total=False):
    """A stored request for a job execution. Includes which
    `type`, arguments and dependencies, and timeouts."""

    type: str
    transforms: list[SerialisedTransformSpec]
    job_id: str
    parent_id: str | None
    args: ArgsType
    dependencies: Mapping[str, Any]
    job_timeout: float | None
    recover_timeout: float | None
    priority: int
    once: bool


@dataclass
class JobInstance[ArgsType, OutputType]:
    """A full job instance, consisting of a JobSpec and JobArguments. This is runnable by
    the Zahir state-machine"""

    spec: JobSpec[ArgsType, OutputType]
    args: JobArguments[ArgsType]

    @property
    def job_id(self) -> str:
        """Convenience property to access the job_id from args."""
        return self.args.job_id

    @property
    def input(self) -> ArgsType:
        """Convenience property to access the args from args (for backwards compatibility)."""
        return self.args.args

    @property
    def dependencies(self) -> "DependencyGroup":
        """Convenience property to access dependencies from args."""
        return self.args.dependencies

    @property
    def priority(self) -> int:
        """Convenience property to access priority from args."""
        return self.args.priority

    def save(self, context: "Context"):
        return {
            "type": self.spec.type,
            "transforms": [trans.save() for trans in self.spec.transforms],
            "job_id": self.args.job_id,
            "parent_id": self.args.parent_id,
            "args": self.args.args,
            "dependencies": self.args.dependencies.save(context),
            "job_timeout": self.args.job_timeout,
            "recover_timeout": self.args.recover_timeout,
            "priority": self.args.priority,
            "once": self.args.once,
        }

    @classmethod
    def load(cls, context: "Context", data: SerialisedJobInstance[ArgsType]):
        spec_type = data["type"]
        base_spec = cast(JobSpec[ArgsType, OutputType], context.scope.get_job_spec(spec_type))

        transforms_data = data.get("transforms", [])

        if transforms_data:
            transform_specs = [TransformSpec.load(trans) for trans in transforms_data]
            spec = JobSpec(
                type=base_spec.type,
                run=base_spec.run,
                transforms=transform_specs,
                recover=base_spec.recover,
                precheck=base_spec.precheck,
            )
            # Apply transforms to get the runnable spec
            spec = spec.apply_transforms(context.scope)
        else:
            # If no transforms, return the original spec
            spec = base_spec

        from zahir.dependencies.group import DependencyGroup

        job_args = JobArguments[ArgsType](
            dependencies=DependencyGroup.load(context, data["dependencies"]),
            args=data["args"],
            job_id=data["job_id"],
            parent_id=data["parent_id"],
            job_timeout=data["job_timeout"],
            recover_timeout=data["recover_timeout"],
            priority=data.get("priority", 0),
            once=data.get("once", False),
        )
        return cls(spec=spec, args=job_args)


# Transforms take a spec and give a new one.
# The args are a dictionary of transform-specific arguments.
type Transform = Callable[[Mapping[str, Any], JobSpec], JobSpec]
