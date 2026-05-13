"""Effects that can be yielded directly by user job generators."""

from dataclasses import dataclass
from typing import Any, ClassVar

from orbis import Effect, Event

from zahir.core.zahir_types import JobSpec


class ZahirJobEffect[ReturnT](Effect[ReturnT], abstract=True):
    """Base class for effects intended to be yielded by job generators."""


class ZahirJobEvent(ZahirJobEffect[None], Event, abstract=True):
    """Base class for fire-and-forget effects yielded by job generators."""


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
class EGetState(ZahirJobEffect[str | None]):
    """Read an arbitrary string value from the runner's key-value store by name.

    Returns None if the key has not been set. Unlike EGetSemaphore, the value
    is not constrained to DependencyState strings.
    """

    tag: ClassVar[str] = "get_state"
    name: str


@dataclass
class ESetState(ZahirJobEvent):
    """Write an arbitrary string value to the runner's key-value store by name.

    Unlike ESetSemaphore, the value is not constrained to DependencyState strings.
    EGetSemaphore / ESetSemaphore are thin wrappers over EGetState / ESetState.
    """

    tag: ClassVar[str] = "set_state"
    name: str
    value: str
