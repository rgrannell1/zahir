from collections import deque
from dataclasses import dataclass, field
from typing import Any, Literal

from zahir.core.constants import DependencyState


class JobContext[T]:
    """Context object passed as the first argument to every job function.

    Runtime fields are injected by the worker process. User state lives in
    user_context, populated by calling the user_context factory passed to evaluate().
    """

    __slots__ = ("_scope", "scope", "handler_wrappers", "user_context")

# Result types for dependency combinators — the Left/Right of the dependency monad.
type Satisfied = tuple[Literal["satisfied"], dict | None]
type Impossible = tuple[Literal["impossible"], str]
type DependencyResult = Satisfied | Impossible

# Concurrency slot tracking: name -> (limit, active_count)
type ConcurrencyMap = dict[str, tuple[int, int]]

# Per-worker buffered results: worker_pid_bytes -> deque of (sequence_number, body)
type PendingResults = dict[bytes, deque[tuple[int, Any]]]

# Effect tag -> handler callable, as returned by handler factory functions
type HandlerMap = dict[str, Any]


@dataclass
class JobSpec:
    """Describes a job to be dispatched: what to call and routing info for the reply."""

    fn_name: str
    args: tuple[Any, ...] = field(default_factory=tuple)
    timeout_ms: int | None = None
    reply_to: bytes | None = None
    sequence_number: int | None = None


