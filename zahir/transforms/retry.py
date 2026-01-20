from collections.abc import Generator, Mapping
from typing import Any, cast

from zahir.base_types import Context, JobEventSet, JobInstance, JobSpec
from zahir.dependencies.group import DependencyGroup
from zahir.events import Await, JobOutputEvent
from zahir.jobs.sleep import Sleep


def retry[ArgsType, OutputType](
    args: Mapping[str, Any], spec: JobSpec[ArgsType, OutputType]
) -> JobSpec[ArgsType, OutputType]:
    """Transform a JobSpec to add retry logic with exponential backoff.

    This works by running the original job as an Awaited child job, which allows
    us to catch exceptions via try/except. On failure, we wait with exponential
    backoff and retry. On success, we forward the output.

    @param args: Transform arguments:
        - max_retries (int): Maximum number of retry attempts (default: 3)
        - backoff_factor (float): Base backoff time in seconds (default: 1.0)
    @param spec: The JobSpec to transform
    @return: A new JobSpec with retry logic wrapping the original job
    """

    max_retries = args.get("max_retries", 3)
    backoff_factor = args.get("backoff_factor", 1.0)

    # Store the original spec's type and run function.
    # We'll look up the base spec from scope when running to create child jobs.
    original_type = spec.type

    def retry_run(
        context: Context, job_args: ArgsType, dependencies: DependencyGroup
    ) -> Generator[JobEventSet[OutputType], Any, Any]:
        last_error: Exception | None = None

        # Get the base spec from scope (without transforms applied)
        # Cast to preserve the generic types
        base_spec = cast(JobSpec[ArgsType, OutputType], context.scope.get_job_spec(original_type))

        for attempt in range(max_retries + 1):
            try:
                # Run the original job as an awaited child job (to catch exceptions)
                extended_dependencies = dependencies.request_extension(backoff_factor * (2**max_retries) * 2)

                # Create child job from base spec (no transforms)
                child_job: JobInstance[ArgsType, OutputType] = base_spec(job_args, extended_dependencies)
                result = yield Await(child_job)

                # Only forward output if child produced one
                if result is not None:
                    yield JobOutputEvent(output=result)
                else:
                    return

            except Exception as err:
                last_error = err
                if attempt < max_retries:
                    backoff_time = backoff_factor * (2**attempt)
                    # Wait for the backoff period before retrying
                    _ = yield Await(Sleep({"duration_seconds": backoff_time}, {}))
                    # Continue to next iteration to retry
                else:
                    raise last_error

    return JobSpec(
        type=spec.type,
        precheck=spec.precheck,
        recover=spec.recover,
        run=retry_run,
    )
