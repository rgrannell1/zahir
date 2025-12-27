"""Retry

A task for retrying another task under certain conditions.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Iterator, TypedDict
from zahir.dependencies.job import JobDependency
from zahir.dependencies.group import DependencyGroup
from zahir.dependencies.time import TimeDependency
from zahir.base_types import Context, Dependency, JobOptions, JobState, Job


class RetryOptions(TypedDict, total=False):
    """Configuration options for retry behavior."""

    max_retries: int
    backoff_factor: float
    initial_delay: float


class RetryTaskInput(TypedDict):
    """Input structure for RetryTask."""

    type: str
    input: dict[str, Any]
    dependencies: Any
    options: JobOptions | None
    retry_options: RetryOptions
    retry_count: int
    retry_states: set[JobState]
    impossible_states: set[JobState]


def create_backoff_delay(retry_opts: RetryOptions, retry_count: int) -> TimeDependency:
    """Create a TimeDependency with exponential backoff delay.

    Args:
        retry_opts: Dictionary containing 'initial_delay' and 'backoff_factor'
        retry_count: The current retry attempt number (0-indexed)

    Returns:
        TimeDependency that will be satisfied after the calculated delay
    """
    initial_delay = retry_opts.get("initial_delay", 5)
    backoff_factor = retry_opts.get("backoff_factor", 2)

    # delay = initial_delay * (backoff_factor ^ retry_count)
    delay_seconds = initial_delay * (backoff_factor**retry_count)

    now = datetime.now(tz=timezone.utc)
    after_time = now + timedelta(seconds=delay_seconds)

    return TimeDependency(before=None, after=after_time)


class RetryTask(Job):
    """Retry a task upon failure"""

    def __init__(
        self,
        input: RetryTaskInput,
        dependencies: dict[str, Dependency | list[Dependency]],
    ):
        super().__init__(input, dependencies)

    @classmethod
    def run(
        cls, context: Context, input: RetryTaskInput, dependencies: DependencyGroup
    ) -> Iterator[Job]:
        """Run the retry task."""

        # Reconstruct the task from serialized data
        task = Job.load(
            context,
            {
                "type": input["type"],
                "job_id": "",  # Will be regenerated
                "parent_id": None,
                "input": input["input"],
                "dependencies": input["dependencies"],
                "options": input["options"].save() if input["options"] else None,
            },
        )

        failure_sensor: JobDependency = JobDependency(
            task.job_id,
            context.job_registry,
            satisfied_states=input["retry_states"],
            impossible_states=input["impossible_states"],
        )
        success_sensor: JobDependency = JobDependency(
            task.job_id,
            context.job_registry,
            impossible_states={JobState.COMPLETED},
        )

        retry_opts = input["retry_options"]
        retry_count = input["retry_count"]

        delay_sensor = create_backoff_delay(retry_opts, retry_count)

        yield task
        if retry_count >= retry_opts.get("max_retries", 3):
            return

        yield RetryTask(
            {
                "type": type(task).__name__,
                "input": task.input,
                "dependencies": task.dependencies,
                "options": task.options,
                "retry_options": input["retry_options"],
                "retry_count": input["retry_count"] + 1,
                "retry_states": input["retry_states"],
                "impossible_states": input["impossible_states"],
            },
            {
                "failure_sensor": failure_sensor,
                "success_sensor": success_sensor,
                "delay_sensor": delay_sensor,
            },
        )


def retryable(
    context: Context,
    task: Job,
    retry_opts: RetryOptions | None = None,
    retry_states: set[JobState] = {
        JobState.TIMED_OUT,
        JobState.RECOVERY_TIMED_OUT,
        JobState.IRRECOVERABLE,
        JobState.IMPOSSIBLE,
    },
    impossible_states: set[JobState] = {JobState.COMPLETED},
) -> Iterable[Job]:
    """Retry a job if it enters a failure state."""
    yield task

    if retry_opts is None:
        resolved_retry_opts: RetryOptions = {
            "max_retries": 3,
            "backoff_factor": 2,
            "initial_delay": 5,
        }
    else:
        resolved_retry_opts = retry_opts

    failure_sensor: JobDependency = JobDependency(
        task.job_id,
        context.job_registry,
        satisfied_states=retry_states,
        impossible_states=impossible_states,
    )
    success_sensor: JobDependency = JobDependency(
        task.job_id,
        context.job_registry,
        impossible_states={JobState.COMPLETED},
    )

    delay_sensor = create_backoff_delay(resolved_retry_opts, 0)

    # on failure, yield the retry task. This task will block until a
    # failure state is reached. It should also be programmed to be impossible if other states are reached to
    retry_input: RetryTaskInput = {
        "type": type(task).__name__,
        "input": task.input,
        "dependencies": task.dependencies,
        "options": task.options,
        "retry_options": resolved_retry_opts,
        "retry_count": 0,
        "retry_states": retry_states,
        "impossible_states": impossible_states,
    }
    yield RetryTask(
        retry_input,
        {
            # our dependencies; only run if:
            # * the upstream job fails
            # * wait a little before retrying
            # and
            # * bail if the upstream job is successful (TODO)
            "failure_sensor": failure_sensor,
            "success_sensor": success_sensor,
            "delay_sensor": delay_sensor,
        },
    )
