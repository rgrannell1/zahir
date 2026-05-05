"""Constants for the Zahir workflow engine"""

from enum import StrEnum
from tertius import EReceive
from zahir.core.exceptions import InvalidEffectError, JobError, JobTimeoutError


# How often should we poll for dependency state-changes?
DEPENDENCY_DELAY_MS = 1_000

# How long should idle workers pause before polling for new work?
WORKER_POLL_MS = 100

# How long to pause while checking for completion
COMPLETION_POLL_MS = 500

# How long to wait between CPU usage samples when checking resource availability
CPU_SAMPLE_INTERVAL_S = 0.1

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
    # emitted when a worker picks up a new job; carries the correct executing worker pid
    EXECUTE = "job:execute"
    # span event covering full job lifetime from enqueue to completion, for duration aggregation
    JOB_LIFECYCLE = "job_lifecycle"


# Semaphore and dependency status values — the three states a dependency or semaphore can be in
class DependencyState(StrEnum):
    SATISFIED = "satisfied"
    UNSATISFIED = "unsatisfied"
    IMPOSSIBLE = "impossible"


# Tags on telemetry events emitted by the dependency polling loop
class DependencyTag(StrEnum):
    WAITING = "dep:waiting"  # emitted on each poll that returns unsatisfied
    SATISFIED = "dep:satisfied"  # emitted when the dependency is finally met or abandoned


# Tags on work items returned by the overseer to workers
class WorkItemTag(StrEnum):
    JOB = "job"
    RESULT = "result"


# Exceptions that evaluate_job throws back into the running job rather than propagating outward
THROWABLE = (JobTimeoutError, JobError, InvalidEffectError)

# Tertius effects that must not be yielded directly by a zahir job — they block the worker process indefinitely
BLOCKED_EFFECTS = (EReceive,)
