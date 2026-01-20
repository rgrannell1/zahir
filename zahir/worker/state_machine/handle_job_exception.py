from zahir.base_types import JobState
from zahir.worker.state_machine.states import CheckPreconditionsStateChange


def handle_job_exception(state) -> tuple[CheckPreconditionsStateChange, None]:
    """The job raised an exception. Emit a recovery started event, and switch to recovery mode."""

    frame = state.frame

    # Let's fork execution back to the job's recovery mechanism
    # we keep the same process ID. We should update the job registry.
    frame.job_generator = frame.job.spec.recover(
        state.context, frame.job.input, frame.job.dependencies, state.last_event
    )

    frame.recovery = True
    state.context.job_registry.set_state(
        state.context,
        frame.job.job_id,
        frame.job.spec.type,
        state.workflow_id,
        state.output_queue,
        JobState.RECOVERING,
        recovery=frame.recovery,
    )
    job_type = frame.job_type()

    return CheckPreconditionsStateChange({"message": f"Job {job_type} entering recovery"}), state
