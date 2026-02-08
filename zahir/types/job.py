"""Job-related types.

Contains JobSpec, JobInstance, JobArguments, the JobEventSet type
alias, and associated type aliases and validation functions.

This module imports from the other zahir.types submodules and uses
TYPE_CHECKING for the circular reference to events.
"""

from collections.abc import Callable, Generator, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypeVar, cast

from typeguard import check_type

from zahir.exception import JobPostcheckError, JobPrecheckError
from zahir.types.context import Context
from zahir.types.scope import Scope
from zahir.types.serialised import SerialisedJobData, SerialisedJobInstance
from zahir.types.transform import TransformSpec
from zahir.utils.id_generator import generate_job_id

if TYPE_CHECKING:
    from zahir.dependencies.group import DependencyGroup
    from zahir.events import Await, JobOutputEvent, WorkflowOutputEvent, ZahirCustomEvent

from zahir.types.dependency import Dependency

# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Type Variables
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

ArgsType = TypeVar("ArgsType", bound=dict)
OutputType = TypeVar("OutputType", bound=dict)


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Type Aliases
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


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


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Helper Functions
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


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
            exc = JobPrecheckError("TypedDict validation failed")
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
        exc = JobPostcheckError("TypedDict output validation failed")
        exc.__cause__ = err
        return exc


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# JobSpec
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


@dataclass
class JobSpec[ArgsType, OutputType]:
    """The specification for a job; how it runs, recovers, and prechecks. JobSpecs
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
        """Serialise the job spec to a dictionary.

        @return: The serialised job spec
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
        serialised and re-applied after loading.

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
            transforms=self.transforms,  # Preserve transforms for serialisation
            recover=self.recover,
            precheck=self.precheck,
            args_type=self.args_type,
            output_type=self.output_type,
        )

        # ...then, apply each transform in order
        for transform_spec in self.transforms:
            transform_fn = scope.get_transform(transform_spec.type)
            result = transform_fn(transform_spec.args, result)

            # Keep the transforms list on the result for serialisation
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


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# JobArguments
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


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


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# JobInstance
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


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
                args_type=base_spec.args_type,
                output_type=base_spec.output_type,
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
