from zahir.base_types import JobState
from zahir.exception import JobRecoveryTimeoutError
from zahir.worker.state_machine.states import EnqueueJobStateChange


def handle_recovery_job_timeout(state) -> tuple[EnqueueJobStateChange, None]:
    """The recovery job timed out. Emit a recovery timeout event, null out the job, and start over."""

    error = JobRecoveryTimeoutError("Recovery job execution timed out")

    state.context.job_registry.set_state(
        state.frame.job.job_id, state.workflow_id, state.output_queue, JobState.RECOVERY_TIMED_OUT, error=error
    )
    job_type = state.frame.job_type()
    state.frame = None

    return EnqueueJobStateChange({"message": f"Recovery job {job_type} timed out"}), state
