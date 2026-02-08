"""Job state definitions.

Contains the JobState enum, terminal/active state sets, and
coverage validation. No internal Zahir imports needed.
"""

from enum import StrEnum


class JobState(StrEnum):
    """Track the state jobs can be in"""

    # Still to be run
    PENDING = "pending"

    # Dependencies have not yet been satisfied for this job
    BLOCKED = "blocked"

    # Dependencies can never be satisfied, so
    # this job is impossible to run
    IMPOSSIBLE = "impossible"

    # Ready to run
    READY = "ready"

    # Claimed, now paused
    PAUSED = "paused"

    # Input validations for the job failed
    PRECHECK_FAILED = "precheck_failed"

    # Currently running
    RUNNING = "running"

    # Completed successfully
    COMPLETED = "completed"

    # Recovery running
    RECOVERING = "recovering"

    # Recovered successfully
    RECOVERED = "recovered"

    # Execution timed out
    TIMED_OUT = "timed_out"

    # Recovery timed out
    RECOVERY_TIMED_OUT = "recovery_timed_out"

    # Even rollback failed; this job is irrecoverable
    IRRECOVERABLE = "irrecoverable"


# Terminal job-states
COMPLETED_JOB_STATES = {
    JobState.IMPOSSIBLE,
    JobState.PRECHECK_FAILED,
    JobState.COMPLETED,
    JobState.RECOVERED,
    JobState.TIMED_OUT,
    JobState.RECOVERY_TIMED_OUT,
    JobState.IRRECOVERABLE,
}


# Active, non-terminal job-states
ACTIVE_JOB_STATES = {
    JobState.PENDING,
    JobState.BLOCKED,
    JobState.READY,
    JobState.PAUSED,
    JobState.RECOVERING,
    JobState.RUNNING,
}


def check_job_states_coverage() -> None:
    """Ensure that active and completed job states cover all possible job states."""

    # Verify that active and completed states cover all possible states
    total_job_states = set(JobState)

    covered_states = ACTIVE_JOB_STATES.union(COMPLETED_JOB_STATES)
    if covered_states != total_job_states:
        missing_states = total_job_states - covered_states
        raise ValueError(f"Job states coverage is incomplete. Missing states: {missing_states}")


check_job_states_coverage()
