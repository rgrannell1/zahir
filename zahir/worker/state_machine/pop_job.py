from zahir.base_types import JobState
from zahir.worker.state_machine.states import CheckPreconditionsStateChange, ExecuteJobStateChange


def pop_job(state) -> tuple[CheckPreconditionsStateChange | ExecuteJobStateChange, None]:
    """We need a job; pop one off the stack"""

    # different one when we pop a job, which is also fine.
    runnable_frame_idx = state.job_stack.runnable_frame_idx(state.context.job_registry)
    assert runnable_frame_idx is not None

    state.frame = state.job_stack.pop(runnable_frame_idx)

    job_state = state.context.job_registry.get_state(state.frame.job.job_id)

    if job_state == JobState.READY:
        return CheckPreconditionsStateChange(
            {"message": f"Job {type(state.frame.job).__name__} claimed and active"},
        ), state

    return ExecuteJobStateChange(
        {"message": f"Resuming job {type(state.frame.job).__name__} in state '{job_state}'"},
    ), state
