from zahir.base_types import JobState
from zahir.exception import JobPrecheckError
from zahir.worker.state_machine.states import (
    ExecuteJobStateChange,
    ExecuteRecoveryJobStateChange,
    StartStateChange,
)


def check_preconditions(state) -> tuple[ExecuteRecoveryJobStateChange | ExecuteJobStateChange | StartStateChange, None]:
    """Can we even run this job? Check the input preconditions first."""

    errors = type(state.frame.job).precheck(state.frame.job.input)

    if not errors:
        if state.frame.recovery:
            return ExecuteRecoveryJobStateChange({"message": "Prechecks passed; executing recovery job"}), state
        return ExecuteJobStateChange({"message": "Prechecks passed; executing job"}), state

    # we'll avoid re-running for `Paused` jobs.
    # Precheck failed; job is no longer on the stack, so
    # let's report and continue.

    precheck_error = JobPrecheckError("\n".join(errors))

    state.context.job_registry.set_state(
        state.frame.job.job_id,
        state.workflow_id,
        state.output_queue,
        JobState.PRECHECK_FAILED,
        error=precheck_error,
    )

    state.frame = None

    return StartStateChange({"message": "Prechecks failed; cleared job."}), state
