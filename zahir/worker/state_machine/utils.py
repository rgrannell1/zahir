from zahir.base_types import Dependency, JobInstance
from zahir.events import Await
from zahir.jobs import Empty


def process_await[ArgsType, OutputType](await_event: Await[ArgsType, OutputType]) -> Await[ArgsType, OutputType]:
    """Awaits support a mix of dependencies and jobs at the interface level. But in practice, we
    just convert the dependencies to awaiting on Empty jobs. This function does that conversion."""

    data = await_event.job

    if isinstance(data, JobInstance):
        # standard case, return the job

        return await_event
    elif isinstance(data, Dependency):
        # Bind the dependencies to an Empty job.

        dependency_job = Empty({}, {"dependencies": data})
        return Await(dependency_job)
    elif isinstance(data, list):
        # can be a mix of jobs and dependencies.

        jobs: list[JobInstance] = []
        dependencies: list[Dependency] = []

        # Partition to jobs and dependencies.
        for item in data:
            if isinstance(item, JobInstance):
                jobs.append(item)
            elif isinstance(item, Dependency):
                dependencies.append(item)
            else:
                raise ValueError(f"Invalid awaitable item: {item}")

        # Now we've partitioned jobs, construct a dependency job for the dependencies
        # Only create Empty job if there are actually dependencies
        if dependencies:
            dependency_job = Empty({}, {"dependencies": dependencies})
            return Await(jobs + [dependency_job])
        else:
            # No dependencies, just return the jobs
            return Await(jobs)
    else:
        raise ValueError(f"Invalid awaitable: {data}")
