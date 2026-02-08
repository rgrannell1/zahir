from zahir.worker.state_machine.run_job_executor import ExecutionParams, run_job_executor
from zahir.worker.state_machine.states import (
    HandleAwaitStateChange,
    HandleJobCompleteNoOutputStateChange,
    HandleJobExceptionStateChange,
    HandleJobOutputStateChange,
    HandleJobTimeoutStateChange,
    WaitForJobStateChange,
)

NORMAL_EXECUTION = ExecutionParams(
    label="Job",
    skip_if_already_running=True,
    timing_method="time_since_started",
    timeout_field="job_timeout",
    exception_state=HandleJobExceptionStateChange,
    timeout_state=HandleJobTimeoutStateChange,
)


def execute_job(
    state,
) -> tuple[
    HandleJobOutputStateChange
    | HandleAwaitStateChange
    | HandleJobCompleteNoOutputStateChange
    | HandleJobTimeoutStateChange
    | HandleJobExceptionStateChange
    | WaitForJobStateChange,
    None,
]:
    """Execute a job. Handle timeouts, awaits, outputs, exceptions."""

    return run_job_executor(state, NORMAL_EXECUTION)
