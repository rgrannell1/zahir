import os

from zahir.base_types import JobState
from zahir.events import JobWorkerWaitingEvent
from zahir.serialise import serialise_event
from zahir.worker.state_machine.states import WaitForJobStateChange


def handle_recovery_job_exception(state) -> tuple[WaitForJobStateChange, None]:
    """The recovery job raised an exception. Emit an irrecoverable event, null out the job, and wait for next dispatch."""

    # well, recovery didn't work. Ah, well.
    state.context.job_registry.set_state(
        state.context,
        state.frame.job.job_id,
        state.frame.job.spec.type,
        state.workflow_id,
        state.output_queue,
        JobState.IRRECOVERABLE,
        error=state.last_event,
        recovery=state.frame.recovery,
    )

    job_type = state.frame.job_type()
    state.frame = None

    # Signal we're ready for another job
    state.output_queue.put(
        serialise_event(state.context, JobWorkerWaitingEvent(pid=os.getpid(), workflow_id=state.workflow_id))
    )

    return WaitForJobStateChange({"message": f"Recovery job {job_type} irrecoverably failed"}), state
