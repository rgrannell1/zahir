from dataclasses import dataclass, field
from enum import StrEnum


class ZahirJobState(StrEnum):
    """Zahir transitions through these states when executing jobs. States are generally 'deconditionalised';
    when we transition out we transition to a state; the state doesn't if-else on the input in general. This
    makes things more traceable, but leads to duplicated states."""

    START = "start"

    ENQUEUE_JOB = "enqueue_job"
    WAIT_FOR_JOB = "wait_for_job"
    POP_JOB = "pop_job"

    CHECK_PRECONDITIONS = "check_preconditions"
    EXECUTE_JOB = "execute_job"
    EXECUTE_RECOVERY_JOB = "execute_recovery_job"
    HANDLE_AWAIT = "handle_await"

    HANDLE_JOB_OUTPUT = "handle_job_output"

    HANDLE_JOB_COMPLETE_NO_OUTPUT = "handle_job_complete_no_output"
    HANDLE_RECOVERY_JOB_COMPLETE_NO_OUTPUT = "handle_recovery_job_complete_no_output"

    HANDLE_JOB_TIMEOUT = "handle_job_timeout"
    HANDLE_JOB_EXCEPTION = "handle_job_exception"

    HANDLE_RECOVERY_JOB_TIMEOUT = "handle_recovery_job_timeout"
    HANDLE_RECOVERY_JOB_EXCEPTION = "handle_recovery_job_exception"


@dataclass
class StateChange:
    """Represents a state transition in the Zahir job state machine."""

    data: dict
    state: ZahirJobState = field(default=ZahirJobState.START, init=False)


@dataclass
class StartStateChange(StateChange):
    state: ZahirJobState = field(default=ZahirJobState.START, init=False)


@dataclass
class EnqueueJobStateChange(StateChange):
    state: ZahirJobState = field(default=ZahirJobState.ENQUEUE_JOB, init=False)


@dataclass
class WaitForJobStateChange(StateChange):
    state: ZahirJobState = field(default=ZahirJobState.WAIT_FOR_JOB, init=False)


@dataclass
class PopJobStateChange(StateChange):
    state: ZahirJobState = field(default=ZahirJobState.POP_JOB, init=False)


@dataclass
class CheckPreconditionsStateChange(StateChange):
    state: ZahirJobState = field(default=ZahirJobState.CHECK_PRECONDITIONS, init=False)


@dataclass
class ExecuteJobStateChange(StateChange):
    state: ZahirJobState = field(default=ZahirJobState.EXECUTE_JOB, init=False)


@dataclass
class ExecuteRecoveryJobStateChange(StateChange):
    state: ZahirJobState = field(default=ZahirJobState.EXECUTE_RECOVERY_JOB, init=False)


@dataclass
class HandleAwaitStateChange(StateChange):
    state: ZahirJobState = field(default=ZahirJobState.HANDLE_AWAIT, init=False)


@dataclass
class HandleJobOutputStateChange(StateChange):
    state: ZahirJobState = field(default=ZahirJobState.HANDLE_JOB_OUTPUT, init=False)


@dataclass
class HandleJobCompleteNoOutputStateChange(StateChange):
    state: ZahirJobState = field(default=ZahirJobState.HANDLE_JOB_COMPLETE_NO_OUTPUT, init=False)


@dataclass
class HandleRecoveryJobCompleteNoOutputStateChange(StateChange):
    state: ZahirJobState = field(default=ZahirJobState.HANDLE_RECOVERY_JOB_COMPLETE_NO_OUTPUT, init=False)


@dataclass
class HandleJobTimeoutStateChange(StateChange):
    state: ZahirJobState = field(default=ZahirJobState.HANDLE_JOB_TIMEOUT, init=False)


@dataclass
class HandleJobExceptionStateChange(StateChange):
    state: ZahirJobState = field(default=ZahirJobState.HANDLE_JOB_EXCEPTION, init=False)


@dataclass
class HandleRecoveryJobTimeoutStateChange(StateChange):
    state: ZahirJobState = field(default=ZahirJobState.HANDLE_RECOVERY_JOB_TIMEOUT, init=False)


@dataclass
class HandleRecoveryJobExceptionStateChange(StateChange):
    state: ZahirJobState = field(default=ZahirJobState.HANDLE_RECOVERY_JOB_EXCEPTION, init=False)
