from zahir.worker.state_machine.states import CheckPreconditionsStateChange, EnqueueJobStateChange, PopJobStateChange


def start(state) -> tuple[EnqueueJobStateChange | PopJobStateChange | CheckPreconditionsStateChange, None]:
    """Initial state; transition to enqueueing a job."""

    if not state.frame:
        # There is no active job; let's either pop one from the stack if something is runnable or enqueue a new one.
        if state.job_stack.is_empty():
            return EnqueueJobStateChange({"message": "No job; enqueueing."}), state

        # We _could_ run this job by popping it of the frame. It's possible we'll actually run a
        # different one when we pop a job, which is also fine.
        runnable_frame_idx = state.job_stack.runnable_frame_idx(state.context.job_registry)

        if runnable_frame_idx is not None:
            return PopJobStateChange({"message": "No job active, so popping from stack"}), state
        return EnqueueJobStateChange({"message": "No job runnable; enqueueing."}), state

    job_type = state.frame.job_type()

    return CheckPreconditionsStateChange(
        {"message": f"Checking preconditions for '{job_type}'"},
    ), state
