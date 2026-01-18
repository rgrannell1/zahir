import os

from zahir.base_types import JobState
from zahir.events import JobWorkerWaitingEvent
from zahir.serialise import serialise_event
from zahir.worker.state_machine.states import WaitForJobStateChange


def handle_recovery_job_complete_no_output(state) -> tuple[WaitForJobStateChange, None]:
    """Mark the recovery job as complete. Emit a recovery completion event. Null out the job. Wait for next dispatch."""

    # Recovery subjob complete, emit a recovery complete and null out the stack frame.
    state.context.job_registry.set_state(
        state.context, state.frame.job.job_id, state.frame.job.spec.type, state.workflow_id, state.output_queue, JobState.RECOVERED, recovery=state.frame.recovery
    )

    state.frame = None

    # Signal we're ready for another job
    state.output_queue.put(serialise_event(state.context, JobWorkerWaitingEvent(pid=os.getpid())))

    return WaitForJobStateChange({"message": "Recovery job completed with no output"}), state
