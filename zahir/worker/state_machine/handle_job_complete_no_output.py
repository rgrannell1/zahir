import os

from zahir.base_types import JobState
from zahir.events import JobWorkerWaitingEvent
from zahir.worker.state_machine.states import WaitForJobStateChange


def handle_job_complete_no_output(state) -> tuple[WaitForJobStateChange, None]:
    """Mark the job as complete. Emit a completion event. Null out the job. Wait for next dispatch."""

    # Subjob complete, emit a recovery complete or regular compete and null out the stack frame.

    frame = state.frame

    state.context.job_registry.set_state(
        frame.job.job_id, state.workflow_id, state.output_queue, JobState.COMPLETED, recovery=frame.recovery
    )

    state.frame = None

    # Signal we're ready for another job
    state.output_queue.put(JobWorkerWaitingEvent(pid=os.getpid()))

    return WaitForJobStateChange({"message": "Job completed with no output"}), state
