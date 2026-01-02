from zahir.base_types import JobState
from zahir.worker.state_machine.states import EnqueueJobStateChange, StateChange


def handle_recovery_job_exception(state) -> tuple[StateChange, None]:
    """The recovery job raised an exception. Emit an irrecoverable event, null out the job, and start over."""

    # well, recovery didn't work. Ah, well.
    state.context.job_registry.set_state(
        state.frame.job.job_id,
        state.workflow_id,
        state.output_queue,
        JobState.IRRECOVERABLE,
        error=state.last_event,
    )

    job_type = state.frame.job_type()
    state.frame = None

    return EnqueueJobStateChange({"message": f"Recovery job {job_type} irrecoverably failed"}), state
