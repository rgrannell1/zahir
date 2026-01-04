from zahir.base_types import Job, JobState
from zahir.worker.state_machine.states import EnqueueJobStateChange


def handle_await(state) -> tuple[EnqueueJobStateChange, None]:
    """We received an Await event. We should put out current job back on the stack,
    pause the job formally, then load the awaited job and start executing it."""

    job_stack = state.job_stack
    job_registry = state.context.job_registry

    await_event = state.await_event
    assert await_event is not None

    # Update the paused job to await the new job(s)
    awaited_jobs = [await_event.job] if isinstance(await_event.job, Job) else await_event.job

    # Needed to handle types correctly in empty case
    if isinstance(await_event.job, list):
        state.frame.await_many = True

    # Tie the current job to the awaited job(s)
    for awaited_job in awaited_jobs:
        state.frame.required_jobs.add(awaited_job.job_id)

        # The awaited job can just be run through the normal lifecycle, with the caveat that the _awaiting_
        # job needs a marker thart it's awaiting the new job.
        job_registry.add(awaited_job, state.output_queue)

    # Pause the current job, and put it back on the registry. `Paused` jobs
    # are awaiting some job or other to be updated.
    frame_job_id = state.frame.job.job_id
    job_registry.set_state(
        frame_job_id, state.workflow_id, state.output_queue, JobState.PAUSED, recovery=state.frame.recovery
    )
    job_stack.push(state.frame)
    state.frame = None

    return EnqueueJobStateChange(
        {"message": f"Paused job {frame_job_id}, enqueuing awaited job(s), and moving on to something else..."},
    ), state
