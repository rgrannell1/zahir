"""
State changes correspond to events that get emitted.
"""

from zahir.base_types import JobState
from zahir.events import (
    JobCompletedEvent,
    JobIrrecoverableEvent,
    JobPausedEvent,
    JobPrecheckFailedEvent,
    JobRecoveryCompletedEvent,
    JobRecoveryStartedEvent,
    JobRecoveryTimeoutEvent,
    JobStartedEvent,
    JobTimeoutEvent,
    ZahirEvent,
)
from zahir.exception import exception_to_text_blob


def create_state_event(
    state: JobState, workflow_id: str, job_id: str, error: BaseException | None = None
) -> ZahirEvent | None:
    """Create an event based on the job state transition.

    Returns None for states that don't emit events.
    """
    match state:
        case JobState.PENDING:
            # Not interesting enough to emit an event
            return None
        case JobState.BLOCKED:
            # Not interesting enough to emit an event
            return None
        case JobState.IMPOSSIBLE:
            # Probably should emit an event, but not yet implemented
            return None
        case JobState.READY:
            # Probably should emit an event, but not yet implemented
            return None
        case JobState.PAUSED:
            return JobPausedEvent(workflow_id=workflow_id, job_id=job_id)
        case JobState.PRECHECK_FAILED:
            return JobPrecheckFailedEvent(
                workflow_id=workflow_id, job_id=job_id, error=exception_to_text_blob(error) if error else ""
            )
        case JobState.RUNNING:
            return JobStartedEvent(workflow_id=workflow_id, job_id=job_id)
        case JobState.COMPLETED:
            return JobCompletedEvent(
                job_id=job_id,
                workflow_id=workflow_id,
                duration_seconds=0.1,
            )
        case JobState.RECOVERING:
            return JobRecoveryStartedEvent(
                job_id=job_id,
                workflow_id=workflow_id,
            )
        case JobState.RECOVERED:
            return JobRecoveryCompletedEvent(
                job_id=job_id,
                workflow_id=workflow_id,
                duration_seconds=0.1,
            )
        case JobState.TIMED_OUT:
            return JobTimeoutEvent(
                job_id=job_id,
                workflow_id=workflow_id,
                duration_seconds=0.1,
            )
        case JobState.RECOVERY_TIMED_OUT:
            return JobRecoveryTimeoutEvent(
                job_id=job_id,
                workflow_id=workflow_id,
                duration_seconds=0.1,
            )
        case JobState.IRRECOVERABLE:
            assert error is not None

            return JobIrrecoverableEvent(job_id=job_id, workflow_id=workflow_id, error=error)
