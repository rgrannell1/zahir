from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Generator, Literal, TypedDict


class JobContext[T]:
    """Context object passed as the first argument to every job function."""

    __slots__ = ("_scope", "scope")

    def __init__(self, _scope, scope) -> None:
        self._scope = _scope
        self.scope = scope


# Result types for dependency combinators — the Left/Right of the dependency monad.
type Satisfied = tuple[Literal["satisfied"], dict | None]
type Impossible = tuple[Literal["impossible"], dict | None]
type Unsatisfied = tuple[Literal["unsatisfied"], dict | None]
type ConditionResult = Satisfied | Impossible | Unsatisfied
type DependencyResult = Satisfied | Impossible

# Concurrency slot tracking: name -> (limit, active_count)
type ConcurrencyMap = dict[str, tuple[int, int]]

# Per-worker buffered results: worker_pid_bytes -> deque of (sequence_number, body)
type PendingResults = dict[bytes, deque[tuple[int, Any]]]

# A handler: takes an effect, optionally yields further effects, returns a value
type HandlerCallable = Callable[..., Generator[Any, Any, Any]]


# Effect tag -> handler callable, as returned by handler factory functions
# TODO deprecate this
class HandlerMap(TypedDict):
    __extra_items__: HandlerCallable


class StorageHandlerMap(TypedDict):
    """Handler map for storage effects."""

    __extra_items__: HandlerCallable
    storage_get_job: HandlerCallable
    storage_enqueue: HandlerCallable
    storage_job_done: HandlerCallable
    storage_job_failed: HandlerCallable
    storage_acquire: HandlerCallable
    storage_release: HandlerCallable
    storage_signal: HandlerCallable
    storage_set_semaphore: HandlerCallable
    storage_is_done: HandlerCallable
    storage_get_error: HandlerCallable
    storage_get_result: HandlerCallable


class JobHandlerMap(TypedDict):
    """Handler map for job emittable effects"""

    __extra_items__: HandlerCallable

    acquire: HandlerCallable
    get_semaphore: HandlerCallable
    set_semaphore: HandlerCallable


class CoordinationHandlerMap(TypedDict):
    """Handler map for coordination effects"""

    __extra_items__: HandlerCallable

    acquire_slot: HandlerCallable
    enqueue: HandlerCallable
    get_error: HandlerCallable
    get_job: HandlerCallable
    get_result: HandlerCallable
    is_done: HandlerCallable
    job_complete: HandlerCallable
    job_fail: HandlerCallable
    release: HandlerCallable
    set_semaphore_state: HandlerCallable
    signal: HandlerCallable


@dataclass
class JobSpec:
    """Describes a job to be dispatched: what to call and routing info for the reply."""

    fn_name: str
    args: tuple[Any, ...] = field(default_factory=tuple)
    timeout_ms: int | None = None
    reply_to: bytes | None = None
    sequence_number: int | None = None
