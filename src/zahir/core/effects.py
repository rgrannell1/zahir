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
class JobSpec:
    """Describes a single job to be dispatched: what to call and with what arguments."""

    fn_name: str
    args: tuple[Any, ...] = field(default_factory=tuple)
    timeout_ms: int | None = None


_MISSING = object()


class EAwait(ZahirJobEffect[Any]):
    """Dispatch one or more jobs concurrently.

    Accepts three forms:
      EAwait(ctx.scope.myjob())          — single job, wraps an existing EAwait
      EAwait([ctx.scope.a(), ctx.scope.b()]) — multiple jobs in parallel
      EAwait(jobs=[...], scalar=...)     — internal construction form

    For a single-job dispatch (scalar=True), returns the result directly.
    For a multi-job dispatch (scalar=False), returns results as a list in input order.
    """

    tag: ClassVar[str] = "await"
    jobs: list[JobSpec]
    scalar: bool

    def __init__(self, spec_or_list=_MISSING, *, jobs=None, scalar=True):
        if spec_or_list is not _MISSING:
            if isinstance(spec_or_list, EAwait):
                # EAwait(single_eawait) — passthrough
                self.jobs = spec_or_list.jobs
                self.scalar = spec_or_list.scalar
            elif isinstance(spec_or_list, list):
                # EAwait([eawait, ...]) — multi-dispatch
                self.jobs = [s.jobs[0] for s in spec_or_list]
                self.scalar = False
            else:
                raise TypeError(f"EAwait expects an EAwait or list of EAwait, got {type(spec_or_list).__name__}")
        else:
            # EAwait(jobs=[...], scalar=...) — internal form
            self.jobs = jobs
            self.scalar = scalar

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, EAwait):
            return NotImplemented
        return self.jobs == other.jobs and self.scalar == other.scalar

    def __repr__(self) -> str:
        return f"EAwait(jobs={self.jobs!r}, scalar={self.scalar!r})"


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
class EEnqueue(ZahirCoordinationEffect[None]):
    """Internal: enqueue a child job and route its reply back to this worker."""

    tag: ClassVar[str] = "enqueue"
    fn_name: str
    args: tuple[Any, ...]
    reply_to: bytes  # the requesting worker's PID bytes
    timeout_ms: int | None
    nonce: int  # allocated nonce for routing the reply back to the parent


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
    fn_name: str = ""


@dataclass
class EJobFail(ZahirCoordinationEffect[None]):
    """Internal: report job failure (timeout or error) and route the exception to the caller."""

    tag: ClassVar[str] = "job_fail"
    error: Exception
    reply_to: bytes | None
    nonce: Any
    fn_name: str = ""
