from collections.abc import Mapping
from typing import Any

from zahir.base_types import JobSpec
from zahir.events import Await, JobOutputEvent
from zahir.jobs.sleep import Sleep


def retry(args: Mapping[str, Any], spec: JobSpec) -> JobSpec:
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

    def retry_run(spec_args, context, job_args, dependencies):
        last_error: Exception | None = None

        # Get the base spec from scope (without transforms applied)
        base_spec = context.scope.get_job_spec(original_type)

        for attempt in range(max_retries + 1):
            try:
                # Run the original job as an awaited child job (to catch exceptions)
                extended_dependencies = dependencies.request_extension(
                    backoff_factor * (2 ** max_retries) * 2
                )

                # Create child job from base spec (no transforms)
                child_job = base_spec(job_args, extended_dependencies)
                result = yield Await(child_job)

                # Only forward output if child produced one
                if result is not None:
                    yield JobOutputEvent(result)
                return

            except Exception as err:
                last_error = err
                if attempt < max_retries:
                    backoff_time = backoff_factor * (2 ** attempt)
                    yield Await(Sleep({"duration_seconds": backoff_time}, {}))
                else:
                    # Exhausted retries, re-raise the last error
                    raise err

        # Should not reach here, but just in case
        if last_error:
            raise last_error

    return JobSpec(
        type=spec.type,
        precheck=spec.precheck,
        recover=spec.recover,
        run=retry_run,
    )
