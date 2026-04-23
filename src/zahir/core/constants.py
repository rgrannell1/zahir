from enum import StrEnum

from tertius import EReceive

from zahir.core.exceptions import InvalidEffect, JobError, JobTimeout


# Lifecycle phase attached to every telemetry event
class Phase(StrEnum):
    START = "start"
    END = "end"
    ERROR = "error"


# Tags identifying job lifecycle coordination effects
class JobTag(StrEnum):
    ENQUEUE = "enqueue"
    JOB_COMPLETE = "job_complete"
    JOB_FAIL = "job_fail"

DEPENDENCY_DELAY_MS = 5_000
WORKER_POLL_MS = 100
COMPLETION_POLL_MS = 200

# Semaphore and dependency status values — the three states a dependency or semaphore can be in
class SemaphoreState(StrEnum):
    SATISFIED = "satisfied"
    UNSATISFIED = "unsatisfied"
    IMPOSSIBLE = "impossible"

# How long to wait between CPU usage samples when checking resource availability
CPU_SAMPLE_INTERVAL_S = 0.1

# Messages exchanged between workers and the overseer GenServer
class OverseerMessage(StrEnum):
    GET_JOB = "get_job"
    ENQUEUE = "enqueue"
    JOB_DONE = "job_done"
    JOB_FAILED = "job_failed"
    IS_DONE = "is_done"
    GET_ERROR = "get_error"
    GET_RESULT = "get_result"
    ACQUIRE = "acquire"
    RELEASE = "release"
    SIGNAL = "signal"
    SET_SEMAPHORE = "set_semaphore"

# Exceptions that evaluate_job throws back into the running job rather than propagating outward
THROWABLE = (JobTimeout, JobError, InvalidEffect)

# Tertius effects that must not be yielded directly by a zahir job — they block the worker process indefinitely
BLOCKED_EFFECTS = (EReceive,)
