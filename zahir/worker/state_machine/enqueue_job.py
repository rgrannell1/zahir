import logging
import os
import time

from zahir.worker.call_frame import ZahirStackFrame
from zahir.worker.state_machine.states import StartStateChange, WaitForJobStateChange

log = logging.getLogger(__name__)


def enqueue_job(state) -> tuple[WaitForJobStateChange | StartStateChange, None]:
    """ """

    # Try claiming a few times before giving up and waiting
    # This helps when jobs are becoming available rapidly
    max_retries = 3
    for attempt in range(max_retries):
        job = state.context.job_registry.claim(state.context, str(os.getpid()))

        if job is not None:
            break

        # Sometimes a job is none because we claimed too quickly after another worker
        if attempt < max_retries - 1:
            time.sleep(0.1)
    else:
        log.debug(f"Worker {os.getpid()} failed to claim job after {max_retries} attempts")
        return WaitForJobStateChange({"message": "no pending job to claim, so waiting for one to appear"}), state

    job_generator = type(job).run(state.context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)

    # new top-level job is not awaited by anything on the stack
    # and it's not recovering, it's healthy
    state.job_stack.push(frame)

    return StartStateChange({"message": "Appended new job to stack; going to start"}), state
