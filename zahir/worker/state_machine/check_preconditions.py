from zahir.base_types import JobState
from zahir.exception import JobPrecheckError
from zahir.worker.state_machine.states import (
    ExecuteJobStateChange,
    ExecuteRecoveryJobStateChange,
    HandleJobTimeoutStateChange,
    HandleRecoveryJobTimeoutStateChange,
    StartStateChange,
)


def check_preconditions(
    state,
) -> tuple[
    ExecuteRecoveryJobStateChange
    | ExecuteJobStateChange
    | StartStateChange
    | HandleRecoveryJobTimeoutStateChange
    | HandleJobTimeoutStateChange,
    None,
]:
    """Can we even run this job? Check the input preconditions first."""

    errors = type(state.frame.job).precheck(state.frame.job.input)

    if not errors:
        # Precheck can be slow; we shouldn't transition to the run-states if the job
        # has now timed-out

        job = state.frame.job

        # We have two types of timeout, look up the appropriate one. We
        # timeout based not only on job options if present.
        timeout: float | None = None
        if job.job_options:
            timeout = job.job_options.recover_timeout if state.frame.recovery else job.job_options.job_timeout

        # TO-DO start-time needs to be reset for recovery.
        job_timing = state.context.job_registry.get_job_timing(job.job_id)
        time_since_start = (
            job_timing.time_since_recovery_started() if state.frame.recovery else job_timing.time_since_started()
        )

        if timeout is not None and time_since_start is not None and time_since_start >= timeout:
            # The job has timed out, let's deal with that

            if state.frame.recovery:
                return HandleRecoveryJobTimeoutStateChange({
                    "message": f"Prechecks passed but recovery job {job.job_id} that has already timed out"
                }), state
            else:
                return HandleJobTimeoutStateChange({
                    "message": f"Prechecks passed but job {job.job_id} that has already timed out"
                }), state

        if state.frame.recovery:
            return ExecuteRecoveryJobStateChange({"message": "Prechecks passed; executing recovery job"}), state
        return ExecuteJobStateChange({"message": "Prechecks passed; executing job"}), state

    # we'll avoid re-running for `Paused` jobs.
    # Precheck failed; job is no longer on the stack, so
    # let's report and continue.

    precheck_error = JobPrecheckError("\n".join(errors))

    state.context.job_registry.set_state(
        state.frame.job.job_id,
        state.workflow_id,
        state.output_queue,
        JobState.PRECHECK_FAILED,
        error=precheck_error,
    )

    state.frame = None

    return StartStateChange({"message": "Prechecks failed; cleared job."}), state
