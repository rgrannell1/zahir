"""Effects that can be yielded directly by user job generators."""

from dataclasses import dataclass
from typing import Any, ClassVar, LiteralString

from orbis import Effect, Event

from zahir.core.zahir_types import JobSpec


class ZahirJobEffect[ReturnT](Effect[ReturnT], abstract=True):
    """Base class for effects intended to be yielded by job generators."""


class ZahirJobEvent(ZahirJobEffect[None], Event, abstract=True):
    """Base class for fire-and-forget effects yielded by job generators."""


class EAwait(ZahirJobEffect[Any]):
    """Dispatch one or more jobs concurrently.

    Constructed by ScopeProxy (single job, scalar=True), await_all (multiple
    jobs, scalar=False), and gather_all (multiple jobs, gather=True). User code
    yields ctx.scope.myjob(), await_all([...]), or gather_all([...]).

    For a single-job dispatch (scalar=True), returns the result directly.
    For a multi-job dispatch (scalar=False), returns results as a list in input order;
    the first child failure is thrown into the awaiting job.
    For a gather dispatch (gather=True), returns a list of Ok/Err results in input
    order and never throws — child failures arrive as Err values.
    """

    tag: ClassVar[LiteralString] = "await"
    jobs: list[JobSpec]
    scalar: bool
    gather: bool

    def __init__(self, *, jobs: list[JobSpec], scalar: bool = True, gather: bool = False):
        self.jobs = jobs
        self.scalar = scalar
        self.gather = gather

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, EAwait):
            return NotImplemented
        return (
            self.jobs == other.jobs
            and self.scalar == other.scalar
            and self.gather == other.gather
        )

    def __repr__(self) -> str:
        return f"EAwait(jobs={self.jobs!r}, scalar={self.scalar!r}, gather={self.gather!r})"


def await_all(specs: list[EAwait]) -> EAwait:
    """Dispatch multiple jobs concurrently; returns results as a list in input order."""
    return EAwait(jobs=[spec.jobs[0] for spec in specs], scalar=False)


def gather_all(specs: list[EAwait]) -> EAwait:
    """Dispatch multiple jobs concurrently; returns a list of Ok/Err results in input order.

    Unlike await_all, a child failure does not raise in the awaiting job — each
    child's outcome arrives as an Ok(value) or Err(error) in the returned list.
    """
    return EAwait(jobs=[spec.jobs[0] for spec in specs], scalar=False, gather=True)


@dataclass
class EAcquire(ZahirJobEffect[bool]):
    """Try to acquire a named concurrency slot from the runner's pool.

    Returns True if a slot was available and acquired, False if the pool is full.
    Release is handled automatically when the job exits.
    """

    tag: ClassVar[LiteralString] = "acquire"
    name: str
    limit: int


@dataclass
class EGetState(ZahirJobEffect[str | None]):
    """Read a string value from the runner's key-value store by name. Returns None if unset."""

    tag: ClassVar[LiteralString] = "get_state"
    name: str


@dataclass
class ESetState(ZahirJobEvent):
    """Write a string value to the runner's key-value store by name."""

    tag: ClassVar[LiteralString] = "set_state"
    name: str
    value: str
