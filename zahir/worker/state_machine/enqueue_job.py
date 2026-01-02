import os

from zahir.worker.call_frame import ZahirStackFrame
from zahir.worker.state_machine.states import StartStateChange, WaitForJobStateChange


def enqueue_job(state) -> tuple[WaitForJobStateChange | StartStateChange, None]:
    """ """

    job = state.context.job_registry.claim(state.context, str(os.getpid()))

    if job is None:
        return WaitForJobStateChange({"message": "no pending job to claim, so waiting for one to appear"}), state

    job_generator = type(job).run(state.context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)

    # new top-level job is not awaited by anything on the stack
    # and it's not recovering, it's healthy
    state.job_stack.push(frame)

    return StartStateChange({"message": "Appended new job to stack; going to start"}), state
