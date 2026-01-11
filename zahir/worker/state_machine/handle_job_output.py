import os

from zahir.events import JobWorkerWaitingEvent
from zahir.worker.state_machine.states import StartStateChange


def handle_job_output(state) -> tuple[StartStateChange, None]:
    """We received a job output! It's emitted upstream already; just null out the job state. Persist the output to the state if awaited; we'll pop, then pass
    the output to the awaiting job"""

    state.context.job_registry.set_output(
        state.frame.job.job_id,
        state.workflow_id,
        state.output_queue,
        state.last_event.output,
        recovery=state.frame.recovery,
    )

    # Jobs can only output once. Once we're outputted, we're done.
    # So clear the job and generator
    state.frame = None

    # Signal we're ready for another job
    state.output_queue.put(JobWorkerWaitingEvent(pid=os.getpid()))

    return StartStateChange({"message": "Setting job output"}), state
