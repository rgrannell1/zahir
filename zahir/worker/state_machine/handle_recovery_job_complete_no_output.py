from zahir.base_types import JobState
from zahir.worker.state_machine.states import EnqueueJobStateChange, StateChange


def handle_recovery_job_complete_no_output(state) -> tuple[StateChange, None]:
    """Mark the recovery job as complete. Emit a recovery completion event. Null out the job. Start over."""

    # Recovery subjob complete, emit a recovery complete and null out the stack frame.
    state.context.job_registry.set_state(
        state.frame.job.job_id, state.workflow_id, state.output_queue, JobState.RECOVERED
    )

    state.frame = None

    return EnqueueJobStateChange({"message": "Recovery job completed with no output"}), state
