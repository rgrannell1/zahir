from zahir.base_types import JobState
from zahir.worker.state_machine.states import EnqueueJobStateChange


def handle_job_complete_no_output(state) -> tuple[EnqueueJobStateChange, None]:
    """Mark the job as complete. Emit a completion event. Null out the job. Start over."""

    # Subjob complete, emit a recovery complete or regular compete and null out the stack frame.

    frame = state.frame

    state.context.job_registry.set_state(
        frame.job.job_id, state.workflow_id, state.output_queue, JobState.COMPLETED, recovery=frame.recovery
    )

    frame = None

    return EnqueueJobStateChange({"message": "Job completed with no output"}), state
