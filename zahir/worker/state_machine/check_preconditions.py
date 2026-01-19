from zahir.base_types import JobState
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

    precheck_err: Exception | ExceptionGroup | None = None
    try:
        job = state.frame.job
        precheck_err = job.spec.precheck(None, job.input) if job.spec.precheck else None
    except Exception as err:
        # Things can fail, expecially in a precheck step.
        # This should also capture thrown exception or exception groups,
        # so either approach works.
        precheck_err = err

    if precheck_err is not None:
        # we'll avoid re-running for `Paused` jobs.
        # Precheck failed; job is no longer on the stack, so
        # let's report and continue.

        state.context.job_registry.set_state(
            state.context,
            state.frame.job.job_id,
            state.frame.job.spec.type,
            state.workflow_id,
            state.output_queue,
            JobState.PRECHECK_FAILED,
            recovery=state.frame.recovery,
            error=precheck_err,
        )

        state.frame = None
        return StartStateChange({"message": "Prechecks failed; cleared job."}), state

    # Precheck can be slow; we shouldn't transition to the run-states if the job
    # has now timed-out

    job = state.frame.job

    # We have two types of timeout, look up the appropriate one. We
    # timeout based not only on job options if present.
    timeout = job.args.recover_timeout if state.frame.recovery else job.args.job_timeout

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
        return HandleJobTimeoutStateChange({
            "message": f"Prechecks passed but job {job.job_id} that has already timed out"
        }), state

    if state.frame.recovery:
        return ExecuteRecoveryJobStateChange({"message": "Prechecks passed; executing recovery job"}), state
    return ExecuteJobStateChange({"message": "Prechecks passed; executing job"}), state
