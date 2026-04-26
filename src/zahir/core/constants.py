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
    # emitted when a worker picks up a new job — carries the correct executing-worker pid
    EXECUTE = "job:execute"

DEPENDENCY_DELAY_MS = 5_000
WORKER_POLL_MS = 100
COMPLETION_POLL_MS = 200

# Semaphore and dependency status values — the three states a dependency or semaphore can be in
class DependencyState(StrEnum):
    SATISFIED = "satisfied"
    UNSATISFIED = "unsatisfied"
    IMPOSSIBLE = "impossible"

# How long to wait between CPU usage samples when checking resource availability
CPU_SAMPLE_INTERVAL_S = 0.1

# Tags on telemetry events emitted by the dependency polling loop
class DependencyTag(StrEnum):
    WAITING = "dep:waiting"     # emitted on each poll that returns unsatisfied
    SATISFIED = "dep:satisfied"  # emitted when the dependency is finally met or abandoned

# Tags on work items returned by the overseer to workers
class WorkItemTag(StrEnum):
    JOB = "job"
    RESULT = "result"

# Exceptions that evaluate_job throws back into the running job rather than propagating outward
THROWABLE = (JobTimeout, JobError, InvalidEffect)

# Tertius effects that must not be yielded directly by a zahir job — they block the worker process indefinitely
BLOCKED_EFFECTS = (EReceive,)
