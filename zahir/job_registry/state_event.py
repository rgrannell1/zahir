"""
State changes correspond to events that get emitted.
"""

from zahir.base_types import JobState, JobTimingInformation
from zahir.events import (
    JobCompletedEvent,
    JobImpossibleEvent,
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
    state: JobState,
    workflow_id: str,
    job_id: str,
    job_type: str,
    error: Exception | None = None,
    timing: JobTimingInformation | None = None,
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
            return JobImpossibleEvent(workflow_id=workflow_id, job_id=job_id, job_type=job_type)
        case JobState.READY:
            # Not interesting enough to emit an event
            return None
        case JobState.PAUSED:
            return JobPausedEvent(workflow_id=workflow_id, job_id=job_id, job_type=job_type)
        case JobState.PRECHECK_FAILED:
            return JobPrecheckFailedEvent(
                workflow_id=workflow_id,
                job_id=job_id,
                job_type=job_type,
                error=exception_to_text_blob(error) if error else "",
            )
        case JobState.RUNNING:
            return JobStartedEvent(workflow_id=workflow_id, job_id=job_id, job_type=job_type)
        case JobState.COMPLETED:
            duration = timing.time_since_started() if timing else 0.0
            return JobCompletedEvent(
                job_id=job_id,
                job_type=job_type,
                workflow_id=workflow_id,
                duration_seconds=duration or 0.0,
            )
        case JobState.RECOVERING:
            return JobRecoveryStartedEvent(
                job_id=job_id,
                job_type=job_type,
                workflow_id=workflow_id,
            )
        case JobState.RECOVERED:
            duration = timing.time_since_recovery_started() if timing else 0.0
            return JobRecoveryCompletedEvent(
                job_id=job_id,
                job_type=job_type,
                workflow_id=workflow_id,
                duration_seconds=duration or 0.0,
            )
        case JobState.TIMED_OUT:
            duration = timing.time_since_started() if timing else 0.0
            return JobTimeoutEvent(
                job_id=job_id,
                job_type=job_type,
                workflow_id=workflow_id,
                duration_seconds=duration or 0.0,
            )
        case JobState.RECOVERY_TIMED_OUT:
            duration = timing.time_since_recovery_started() if timing else 0.0
            return JobRecoveryTimeoutEvent(
                job_id=job_id,
                job_type=job_type,
                workflow_id=workflow_id,
                duration_seconds=duration or 0.0,
            )
        case JobState.IRRECOVERABLE:
            assert error is not None

            return JobIrrecoverableEvent(job_id=job_id, job_type=job_type, workflow_id=workflow_id, error=error)
