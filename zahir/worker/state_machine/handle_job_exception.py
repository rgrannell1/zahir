from zahir.base_types import JobState
from zahir.worker.state_machine.states import CheckPreconditionsStateChange


def handle_job_exception(state) -> tuple[CheckPreconditionsStateChange, None]:
    """The job raised an exception. Emit a recovery started event, and switch to recovery mode."""

    frame = state.frame
    from zahir.base_types import JobInstance

    if isinstance(frame.job, JobInstance):
        # For JobInstance, call the spec's recover function
        frame.job_generator = frame.job.spec.recover(None, state.context, frame.job.input, frame.job.dependencies, state.last_event)
    else:
        # For Job classes, call the classmethod
        job_class = type(frame.job)
        frame.job_generator = job_class.recover(state.context, frame.job.input, frame.job.dependencies, state.last_event)

    frame.recovery = True
    state.context.job_registry.set_state(
        frame.job.job_id,
        state.workflow_id,
        state.output_queue,
        JobState.RECOVERING,
        recovery=frame.recovery,
    )
    job_type = frame.job_type()

    return CheckPreconditionsStateChange({"message": f"Job {job_type} entering recovery"}), state
