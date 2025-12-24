"""Retry

A task for retrying another task under certain conditions.
"""

from datetime import datetime, timedelta
from typing import Iterable, Iterator
from zahir.dependencies.job import JobDependency
from zahir.dependencies.group import DependencyGroup
from zahir.dependencies.time import TimeDependency
from zahir.types import Context, Dependency, JobState, Job


def create_backoff_delay(retry_opts: dict, retry_count: int) -> TimeDependency:
    """Create a TimeDependency with exponential backoff delay.

    Args:
        retry_opts: Dictionary containing 'initial_delay' and 'backoff_factor'
        retry_count: The current retry attempt number (0-indexed)

    Returns:
        TimeDependency that will be satisfied after the calculated delay
    """
    initial_delay = retry_opts.get("initial_delay", 5)
    backoff_factor = retry_opts.get("backoff_factor", 2)

    # Exponential backoff: delay = initial_delay * (backoff_factor ^ retry_count)
    delay_seconds = initial_delay * (backoff_factor ** retry_count)

    now = datetime.now()
    after_time = now + timedelta(seconds=delay_seconds)

    return TimeDependency(before=None, after=after_time)


class RetryTask(Job):
    """Retry a task upon failure"""

    def __init__(
        self, input: dict, dependencies: dict[str, Dependency | list[Dependency]]
    ):
        super().__init__(input, dependencies)

    @classmethod
    def run(
        cls, context: Context, input: dict, dependencies: DependencyGroup
    ) -> Iterator[Job]:
        """Run the retry task."""

        job_type = input["type"]
        job_class = context.scope.get_task_class(job_type)
        task = job_class(
            input=input["input"], dependencies=input["dependencies"], options=input["options"]
        )

        failure_sensor = JobDependency(
            task.job_id,
            context.job_registry,
            satisfied_states=input["retry_states"],
            impossible_states=input["impossible_states"],
        )
        success_sensor = JobDependency(
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
                "retry_count": input["retry_count"] + 1
            },
            {
                "failure_sensor": failure_sensor,
                "success_sensor": success_sensor,
                "delay_sensor": delay_sensor
            }
        )


def retryable(
    context: Context,
    task: Job,
    retry_opts: dict | None = None,
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

    retry_opts = retry_opts or {
        'max_retries': 3,
        'backoff_factor': 2,
        'initial_delay': 5
    }

    failure_sensor = JobDependency(
        task.job_id,
        context.job_registry,
        satisfied_states=retry_states,
        impossible_states=impossible_states,
    )
    success_sensor = JobDependency(
        task.job_id,
        context.job_registry,
        impossible_states={JobState.COMPLETED},
    )

    delay_sensor = create_backoff_delay(retry_opts, 0)

    # on failure, yield the retry task. This task will block until a
    # failure state is reached. It should also be programmed to be impossible if other states are reached to
    yield RetryTask(
        {
            "type": type(task).__name__,
            "input": task.input,
            "dependencies": task.dependencies,
            "options": task.options,
            "retry_options": retry_opts,
            "retry_count": 0,
            "retry_states": retry_states,
            "impossible_states": impossible_states,
        },
        {
            # our dependencies; only run if:
            # * the upstream job fails
            # * wait a little before retrying
            # and
            # * bail if the upstream job is successful (TODO)
            "failure_sensor": failure_sensor,
            "success_sensor": success_sensor,
            "delay_sensor": delay_sensor
        },
    )
