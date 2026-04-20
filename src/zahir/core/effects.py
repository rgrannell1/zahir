from dataclasses import dataclass, field
from typing import Any, ClassVar

from orbis import Effect, Event


class ZahirJobEffect[ReturnT](Effect[ReturnT], abstract=True):
    """Base class for effects intended to be yielded by job generators."""


class ZahirJobEvent(ZahirJobEffect[None], Event, abstract=True):
    """Base class for fire-and-forget effects yielded by job generators."""


class ZahirCoordinationEffect[ReturnT](Effect[ReturnT], abstract=True):
    """Base class for effects yielded by the runtime layer, never by jobs directly."""


@dataclass
class ESatisfied(ZahirJobEvent):
    # Jobs yield this to signal a dependency is satisfied. The handler re-emits it
    # as its return value, so the yielding job also receives it back — dual role.
    tag: ClassVar[str] = "satisfied"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class EImpossible(ZahirJobEvent):
    # Jobs yield this to signal a dependency can never be satisfied. Same dual role as ESatisfied.
    tag: ClassVar[str] = "impossible"
    reason: str


@dataclass
class EAwait(ZahirJobEffect[Any]):
    """Dispatch a child job and block until it completes, returning its result."""

    tag: ClassVar[str] = "await"
    fn_name: str
    args: tuple[Any, ...] = field(default_factory=tuple)
    timeout_ms: int | None = None


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
    state: str  # SATISFIED | UNSATISFIED | IMPOSSIBLE from constants


@dataclass
class EAwaitAll(ZahirJobEffect[list[Any]]):
    """Dispatch multiple child jobs concurrently and return results in input order."""

    tag: ClassVar[str] = "await_all"
    effects: list[EAwait]


@dataclass
class EEnqueue(ZahirCoordinationEffect[None]):
    """Internal: enqueue a child job and route its reply back to this worker."""

    tag: ClassVar[str] = "enqueue"
    fn_name: str
    args: tuple[Any, ...]
    reply_to: bytes  # the requesting worker's PID bytes
    timeout_ms: int | None
    nonce: int | None  # None for EAwait; integer index for EAwaitAll


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
    nonce: Any


@dataclass
class EJobFail(ZahirCoordinationEffect[None]):
    """Internal: report job failure (timeout or error) and route the exception to the caller."""

    tag: ClassVar[str] = "job_fail"
    error: Exception
    reply_to: bytes | None
    nonce: Any
