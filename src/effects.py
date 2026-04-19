from dataclasses import dataclass, field
from typing import Any, ClassVar

from orbis import Effect, Event


@dataclass
class ESatisfied(Event):
    tag: ClassVar[str] = "satisfied"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class EImpossible(Event):
    tag: ClassVar[str] = "impossible"
    reason: str


@dataclass
class EAwait(Effect[Any]):
    """Dispatch a child job and block until it completes, returning its result."""

    tag: ClassVar[str] = "await"
    fn_name: str
    args: tuple = field(default_factory=tuple)
    timeout_ms: int | None = None


@dataclass
class ESignal(Effect[str]):
    """Probe the current state of a named semaphore.

    Returns 'satisfied', 'unsatisfied', or 'impossible'.
    State is managed by the runner's GenServer and can be set externally.
    """

    tag: ClassVar[str] = "signal"
    name: str


@dataclass
class EAcquire(Effect[bool]):
    """Try to acquire a named concurrency slot from the runner's pool.

    Returns True if a slot was available and acquired, False if the pool is full.
    Release is handled automatically when the job exits.
    """

    tag: ClassVar[str] = "acquire"
    name: str
    limit: int


@dataclass
class ESetSemaphore(Event):
    """Set the state of a named semaphore in the runner's GenServer."""

    tag: ClassVar[str] = "set_semaphore"
    name: str
    state: str  # SATISFIED | UNSATISFIED | IMPOSSIBLE from constants


@dataclass
class EAwaitAll(Effect[list[Any]]):
    """Dispatch multiple child jobs concurrently and return results in input order."""

    tag: ClassVar[str] = "await_all"
    effects: list[EAwait]
