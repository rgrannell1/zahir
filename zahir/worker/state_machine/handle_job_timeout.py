import os

from zahir.base_types import JobState
from zahir.events import JobWorkerWaitingEvent
from zahir.exception import JobTimeoutError
from zahir.worker.state_machine.states import WaitForJobStateChange


def handle_job_timeout(state) -> tuple[WaitForJobStateChange, None]:
    """The job timed out. Emit a timeout event, null out the job, and wait for next dispatch."""

    error = JobTimeoutError("Job execution timed out")

    state.context.job_registry.set_state(
        state.frame.job.job_id,
        state.workflow_id,
        state.output_queue,
        JobState.TIMED_OUT,
        error=error,
        recovery=state.frame.recovery,
    )

    job_type = state.frame.job_type()
    state.frame = None

    # Signal we're ready for another job
    state.output_queue.put(JobWorkerWaitingEvent(pid=os.getpid()))

    return WaitForJobStateChange({"message": f"Job {job_type} timed out"}), state
