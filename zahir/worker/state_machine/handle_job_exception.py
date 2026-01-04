from zahir.base_types import JobState
from zahir.worker.state_machine.states import CheckPreconditionsStateChange


def handle_job_exception(state) -> tuple[CheckPreconditionsStateChange, None]:
    """The job raised an exception. Emit a recovery started event, and switch to recovery mode."""

    job_class = type(state.frame.job)

    # Let's fork execution back to the job's recovery mechanism
    # we keep the same process ID. We should update the job registry.
    state.frame.job_generator = job_class.recover(
        state.context, state.frame.job.input, state.frame.job.dependencies, state.last_event
    )

    state.frame.recovery = True
    state.context.job_registry.set_state(
        state.frame.job.job_id,
        state.workflow_id,
        state.output_queue,
        JobState.RECOVERING,
        recovery=state.frame.recovery,
    )
    job_type = state.frame.job_type()

    return CheckPreconditionsStateChange({"message": f"Job {job_type} entering recovery"}), state
