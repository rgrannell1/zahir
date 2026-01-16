import os
import queue

from zahir.events import JobAssignedEvent, JobWorkerWaitingEvent
from zahir.utils.logging_config import get_logger
from zahir.worker.call_frame import ZahirStackFrame
from zahir.worker.state_machine.states import PopJobStateChange, StartStateChange

log = get_logger(__name__)

# How long to wait before checking for runnable jobs on the local stack
WAIT_TIMEOUT_SECONDS = 0.5


def wait_for_job(state) -> tuple[StartStateChange | PopJobStateChange, None]:
    """Wait for the overseer to dispatch a job via the input queue.

    This uses a timeout to periodically check for runnable jobs on the local stack.
    This is necessary because when a child job completes, the parent job on this
    worker's stack may become runnable.
    """
    while True:
        # First, check if we have runnable jobs on our local stack
        runnable_frame_idx = state.job_stack.runnable_frame_idx(state.context.job_registry)
        if runnable_frame_idx is not None:
            # A job on our stack can now be resumed!
            return PopJobStateChange({"message": "Paused job on stack is now runnable"}), state

        # Wait for a job dispatch with timeout, then check stack again
        try:
            event = state.input_queue.get(timeout=WAIT_TIMEOUT_SECONDS)
        except queue.Empty:
            # Timeout - loop back and check for runnable jobs
            continue

        if not isinstance(event, JobAssignedEvent):
            log.warning(f"Received unexpected event type {type(event).__name__}, expected JobAssignedEvent")
            return StartStateChange({"message": "Received unexpected event, retrying"}), state

        # Load the job from the registry
        job_id = event.job_id
        job_registry = state.context.job_registry

        # Fetch job data from registry
        job = None
        for job_info in job_registry.jobs(state.context):
            if job_info.job.job_id == job_id:
                job = job_info.job
                break

        if job is None:
            log.error(f"Job {job_id} not found in registry")
            # Signal ready for another job
            state.output_queue.put(JobWorkerWaitingEvent(pid=os.getpid()))
            return StartStateChange({"message": f"Job {job_id} not found, waiting for another"}), state

        # Create the job generator and stack frame
        # Handle both JobInstance (from @spec decorator) and Job classes
        from zahir.base_types import JobInstance
        if isinstance(job, JobInstance):
            # For JobInstance, call the spec's run function directly
            job_generator = job.spec.run(None, state.context, job.input, job.dependencies)
        else:
            # For Job classes, call the classmethod
            job_generator = type(job).run(state.context, job.input, job.dependencies)

        frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)

        # Push job onto stack
        state.job_stack.push(frame)

        return StartStateChange({"message": f"Received job {job_id}; going to start"}), state
