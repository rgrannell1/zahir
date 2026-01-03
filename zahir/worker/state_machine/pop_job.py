from zahir.base_types import JobState
from zahir.worker.state_machine.states import (
    CheckPreconditionsStateChange,
    ExecuteJobStateChange,
    HandleJobTimeoutStateChange,
    HandleRecoveryJobTimeoutStateChange,
)


def pop_job(
    state,
) -> tuple[
    CheckPreconditionsStateChange
    | ExecuteJobStateChange
    | HandleRecoveryJobTimeoutStateChange
    | HandleJobTimeoutStateChange,
    None,
]:
    """We need a job; pop one off the stack"""

    # different one when we pop a job, which is also fine.
    runnable_frame_idx = state.job_stack.runnable_frame_idx(state.context.job_registry)
    assert runnable_frame_idx is not None

    state.frame = state.job_stack.pop(runnable_frame_idx)
    job = state.frame.job

    job_state = state.context.job_registry.get_state(job.job_id)

    # We have two types of timeout, look up the appropriate one. We
    # timeout based not only on job options if present.
    timeout: float | None = None
    if job.job_options:
        timeout = job.job_options.recover_timeout if state.frame.recovery else job.job_options.job_timeout

    job_timing = state.context.job_registry.get_job_timing(job.job_id)

    time_since_start = (
        job_timing.time_since_recovery_started() if state.frame.recovery else job_timing.time_since_started()
    )

    if timeout is not None and time_since_start is not None and time_since_start >= timeout:
        # The job has timed out, let's deal with that

        if state.frame.recovery:
            return HandleRecoveryJobTimeoutStateChange({
                "message": f"Popped recovery-job {job.job_id} that has already timed out"
            }), state
        else:
            return HandleJobTimeoutStateChange({
                "message": f"Popped job {job.job_id} that has already timed out"
            }), state

    if job_state == JobState.READY:
        return CheckPreconditionsStateChange(
            {"message": f"Job {type(state.frame.job).__name__} claimed and active"},
        ), state

    return ExecuteJobStateChange(
        {"message": f"Resuming job {type(state.frame.job).__name__} in state '{job_state}'"},
    ), state
