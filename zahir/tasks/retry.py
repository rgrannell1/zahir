"""Retry

A task for retrying another task under certain conditions.
"""

from typing import Iterable, Iterator
from zahir.dependencies.job import JobDependency
from zahir.dependencies.group import DependencyGroup
from zahir.types import Context, Dependency, JobState, Job


class RetryTask(Job):
  def __init__(
      self,
      input: dict,
      dependencies: dict[str, Dependency | list[Dependency]]):
    super().__init__(input, dependencies)

  @classmethod
  def run(cls, context: Context, input: dict, dependencies: DependencyGroup) -> Iterator[Job]:
    """Run the retry task."""

    job_input = input['input']
    job_dependencies = input['dependencies']
    job_type = input['type']
    job_options = input['options']
    job_class = context.scope.get_task_class(job_type)

    # TODO: think about how workflow IDs and such should be connected in.
    job_instance = job_class(
      input=job_input,
      dependencies=job_dependencies,
      options=job_options
    )

    # TODO recover should also force a retry

    yield job_instance


def retryable(
    context: Context,
    task: Job,
    retry_states: set[JobState] = {
      JobState.TIMED_OUT,
      JobState.RECOVERY_TIMED_OUT,
      JobState.IRRECOVERABLE,
      JobState.IMPOSSIBLE
    },
    impossible_states: set[JobState] = {
      JobState.COMPLETED
    }
  ) -> Iterable[Job]:
  """Retry a job if it enters a failure state. """
  yield task

  # JobDependency needs job_registry as second parameter, states as third
  failure_sensor = JobDependency(
      task.job_id,
      context.job_registry,
      satisfied_states=retry_states,
      impossible_states=impossible_states
  )

  # TODO add a time-dependency that backs off

  # on failure, yield the retry task. This task will block until a
  # failure state is reached. It should also be programmed to be impossible if other states are reached to
  # avoid infinite polling (TODO)
  yield RetryTask({
    "type": type(task).__name__,
    "input": task.input,
    "dependencies": task.dependencies,
    "options": task.options
  }, {
    # our dependencies; only run if the upstream job fails,
    # bail if the upstream job is successful,
    # and wait a little before retrying
    "failure_sensor": failure_sensor
  })
