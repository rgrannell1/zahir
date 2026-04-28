from dataclasses import dataclass
from typing import Any, ClassVar

from orbis import Effect, Event

from zahir.core.zahir_types import JobSpec


class ZahirJobEffect[ReturnT](Effect[ReturnT], abstract=True):
    """Base class for effects intended to be yielded by job generators."""


class ZahirJobEvent(ZahirJobEffect[None], Event, abstract=True):
    """Base class for fire-and-forget effects yielded by job generators."""


class ZahirCoordinationEffect[ReturnT](Effect[ReturnT], abstract=True):
    """Base class for effects yielded by the runtime layer, never by jobs directly."""


class EAwait(ZahirJobEffect[Any]):
    """Dispatch one or more jobs concurrently.

    Constructed by ScopeProxy (single job, scalar=True) and await_all (multiple
    jobs, scalar=False). User code yields ctx.scope.myjob() or await_all([...]).

    For a single-job dispatch (scalar=True), returns the result directly.
    For a multi-job dispatch (scalar=False), returns results as a list in input order.
    """

    tag: ClassVar[str] = "await"
    jobs: list[JobSpec]
    scalar: bool

    def __init__(self, *, jobs: list[JobSpec], scalar: bool = True):
        self.jobs = jobs
        self.scalar = scalar

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, EAwait):
            return NotImplemented
        return self.jobs == other.jobs and self.scalar == other.scalar

    def __repr__(self) -> str:
        return f"EAwait(jobs={self.jobs!r}, scalar={self.scalar!r})"


def await_all(specs: list[EAwait]) -> EAwait:
    """Dispatch multiple jobs concurrently; returns results as a list in input order."""
    return EAwait(jobs=[spec.jobs[0] for spec in specs], scalar=False)


@dataclass
class EGetSemaphore(ZahirJobEffect[str]):
    """Probe the current state of a named semaphore.

    Returns 'satisfied', 'unsatisfied', or 'impossible'.
    State is managed by the runner's GenServer and can be set externally.
    """

    tag: ClassVar[str] = "get_semaphore"
    name: str


@dataclass
class EAcquire(ZahirJobEffect[bool]):
    """Try to acquire a named concurrency slot from the runner's pool.

    Returns True if a slot was available and acquired, False if the pool is full.
    Release is handled automatically when the job exits.
    """

    tag: ClassVar[str] = "acquire"
    name: str
    limit: int


@dataclass
class ESetSemaphore(ZahirJobEvent):
    """Set the state of a named semaphore in the runner's GenServer."""

    tag: ClassVar[str] = "set_semaphore"
    name: str
    state: str  # DependencyState value from constants


@dataclass
class EEnqueue(ZahirCoordinationEffect[None]):
    """Internal: enqueue a child job and route its reply back to this worker."""

    tag: ClassVar[str] = "enqueue"
    fn_name: str
    args: tuple[Any, ...]
    reply_to: bytes  # the requesting worker's PID bytes
    timeout_ms: int | None
    sequence_number: (
        int  # allocated sequence_number for routing the reply back to the parent
    )


@dataclass
class ERelease(ZahirCoordinationEffect[None]):
    """Internal: release a named concurrency slot back to the overseer."""

    tag: ClassVar[str] = "release"
    name: str


@dataclass
class EGetJob(ZahirCoordinationEffect[Any]):
    """Internal: request work from the overseer — returns a new job, a buffered result, or None."""

    tag: ClassVar[str] = "get_job"
    worker_pid_bytes: bytes = b""


@dataclass
class EAcquireSlot(ZahirCoordinationEffect[bool]):
    """Internal: request a named concurrency slot from the overseer."""

    tag: ClassVar[str] = "acquire_slot"
    name: str
    limit: int


@dataclass
class ESignal(ZahirCoordinationEffect[str]):
    """Internal: query the current state of a named semaphore from the overseer."""

    tag: ClassVar[str] = "signal"
    name: str


@dataclass
class ESetSemaphoreState(ZahirCoordinationEffect[None]):
    """Internal: write a new state for a named semaphore to the overseer."""

    tag: ClassVar[str] = "set_semaphore_state"
    name: str
    state: str


@dataclass
class EJobComplete(ZahirCoordinationEffect[None]):
    """Internal: report successful job completion and route the result to the caller."""

    tag: ClassVar[str] = "job_complete"
    result: Any
    reply_to: bytes | None
    sequence_number: Any
    fn_name: str = ""  # TODO: deprecation candidate — piggybacks fn_name for telemetry; job identity should come from a dedicated event  # noqa: E501


@dataclass
class EJobFail(ZahirCoordinationEffect[None]):
    """Internal: report job failure (timeout or error) and route the exception to the caller."""

    tag: ClassVar[str] = "job_fail"
    error: Exception
    reply_to: bytes | None
    sequence_number: Any
    fn_name: str = ""  # TODO: deprecation candidate — piggybacks fn_name for telemetry; job identity should come from a dedicated event  # noqa: E501


@dataclass
class EIsDone(ZahirCoordinationEffect[bool]):
    """Internal: ask the overseer whether all pending jobs have completed."""

    tag: ClassVar[str] = "is_done"


@dataclass
class EGetError(ZahirCoordinationEffect[Exception | None]):
    """Internal: retrieve the root error from the overseer, if any job failed fatally."""

    tag: ClassVar[str] = "get_error"


@dataclass
class EGetResult(ZahirCoordinationEffect[Any]):
    """Internal: retrieve the root job's return value from the overseer."""

    tag: ClassVar[str] = "get_result"


class ZahirStorageEffect[ReturnT](Effect[ReturnT], abstract=True):
    """Base class for storage effects yielded by the overseer gen_server — handled by the storage backend."""


@dataclass
class EStorageInitialize(ZahirStorageEffect[None]):
    """Seed the backend with the root job on overseer startup."""

    tag: ClassVar[str] = "storage:initialize"
    fn_name: str
    args: tuple[Any, ...]


@dataclass
class EStorageGetJob(ZahirStorageEffect[Any]):
    """Return the next work item for a worker from the backend."""

    tag: ClassVar[str] = "storage:get_job"
    worker_pid_bytes: bytes


@dataclass
class EStorageEnqueue(ZahirStorageEffect[None]):
    """Enqueue a child job and increment pending."""

    tag: ClassVar[str] = "storage:enqueue"
    fn_name: str
    args: tuple[Any, ...]
    reply_to: bytes | None
    timeout_ms: int | None
    sequence_number: int | None


@dataclass
class EStorageJobDone(ZahirStorageEffect[None]):
    """Decrement pending and route a result or error to the parent worker, or store as root result."""

    tag: ClassVar[str] = "storage:job_done"
    reply_to: bytes | None
    sequence_number: Any
    body: Any


@dataclass
class EStorageJobFailed(ZahirStorageEffect[None]):
    """Decrement pending and record a root-level failure."""

    tag: ClassVar[str] = "storage:job_failed"
    error: Exception


@dataclass
class EStorageAcquire(ZahirStorageEffect[bool]):
    """Try to acquire a named concurrency slot."""

    tag: ClassVar[str] = "storage:acquire"
    name: str
    limit: int


@dataclass
class EStorageRelease(ZahirStorageEffect[None]):
    """Release a named concurrency slot."""

    tag: ClassVar[str] = "storage:release"
    name: str


@dataclass
class EStorageSignal(ZahirStorageEffect[Any]):
    """Return the current semaphore state for a name."""

    tag: ClassVar[str] = "storage:signal"
    name: str


@dataclass
class EStorageSetSemaphore(ZahirStorageEffect[None]):
    """Set the semaphore state for a name."""

    tag: ClassVar[str] = "storage:set_semaphore"
    name: str
    state: str


@dataclass
class EStorageIsDone(ZahirStorageEffect[bool]):
    """Return True when all pending jobs have completed."""

    tag: ClassVar[str] = "storage:is_done"


@dataclass
class EStorageGetError(ZahirStorageEffect[Any]):
    """Return the root error, if any."""

    tag: ClassVar[str] = "storage:get_error"


@dataclass
class EStorageGetResult(ZahirStorageEffect[Any]):
    """Return the root job's return value."""

    tag: ClassVar[str] = "storage:get_result"
