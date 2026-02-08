from zahir.worker.state_machine.run_job_executor import ExecutionParams, run_job_executor
from zahir.worker.state_machine.states import (
    HandleAwaitStateChange,
    HandleJobCompleteNoOutputStateChange,
    HandleJobOutputStateChange,
    HandleRecoveryJobExceptionStateChange,
    HandleRecoveryJobTimeoutStateChange,
    WaitForJobStateChange,
)

RECOVERY_EXECUTION = ExecutionParams(
    label="Recovery job",
    skip_if_already_running=False,
    timing_method="time_since_recovery_started",
    timeout_field="recover_timeout",
    exception_state=HandleRecoveryJobExceptionStateChange,
    timeout_state=HandleRecoveryJobTimeoutStateChange,
)


def execute_recovery_job(
    state,
) -> tuple[
    HandleJobOutputStateChange
    | HandleAwaitStateChange
    | HandleJobCompleteNoOutputStateChange
    | HandleRecoveryJobTimeoutStateChange
    | HandleRecoveryJobExceptionStateChange
    | WaitForJobStateChange,
    None,
]:
    """Execute a recovery job. Similar to execute_job, but different eventing on failure/completion."""

    return run_job_executor(state, RECOVERY_EXECUTION)
