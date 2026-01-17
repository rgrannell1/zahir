from dataclasses import dataclass
from zahir.base_types import JobSpec
from zahir.jobs.sleep import Sleep


@dataclass
class RetryArgs:
    max_retries: int
    backoff_factor: float



def retry_spec(retry_args: RetryArgs, spec: JobSpec) -> JobSpec:
    """Get the JobSpec for the Retry job."""

    def retry_run(spec_args, context, args, dependencies):

        for attempt in range(retry_args.max_retries + 1):
            try:
                extended_dependencies = dependencies.request_extension(100)
                yield spec(spec_args, context, args, extended_dependencies)
            except Exception as err:
                if attempt < retry_args.max_retries:
                    backoff_time = retry_args.backoff_factor * (2 ** attempt)
                    yield Sleep({ 'duration_seconds': backoff_time }, {})
                else:
                    raise err

    return JobSpec(
        type=spec.type,
        precheck=spec.precheck,
        recover=spec.recover,
        run=retry_run
    )
