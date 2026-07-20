from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Generator, Literal

from zahir.core.constants import DependencyState


class JobContext[T]:
    """Context object passed as the first argument to every job function."""

    __slots__ = ("_scope", "scope")

    def __init__(self, _scope, scope) -> None:
        self._scope = _scope
        self.scope = scope


# Result types for dependency combinators — the Left/Right of the dependency monad.
# TODO: these are type aliases, not classes — isinstance(result, Satisfied)
#       raises a confusing error. Consider replacing with a proper enum or
#       dataclass so callers can use isinstance naturally.
type Satisfied = tuple[Literal[DependencyState.SATISFIED], dict | None]
type Impossible = tuple[Literal[DependencyState.IMPOSSIBLE], dict | None]
type Unsatisfied = tuple[Literal[DependencyState.UNSATISFIED], dict | None]
type ConditionResult = Satisfied | Impossible | Unsatisfied
type DependencyResult = Satisfied | Impossible

# Concurrency slot tracking: name -> (limit, active_count)
type ConcurrencyMap = dict[str, tuple[int, int]]

# Per-worker buffered results: worker_pid_bytes -> deque of (sequence_number, body)
type PendingResults = dict[bytes, deque[tuple[int, Any]]]

# Per-worker outstanding work handout: worker_pid_bytes -> (lease_id, work item).
# Held until the worker acks the lease on its next get-job request, so a reply
# lost to a heartbeat timeout is re-delivered rather than destroyed.
type LeaseMap = dict[bytes, tuple[int, Any]]

# A handler: takes an effect, optionally yields further effects, returns a value
type HandlerCallable = Callable[..., Generator[Any, Any, Any]]


# Effect tag -> handler callable, as returned by every handler factory function.
# One alias for all layers: which tags a bag holds is decided by its factory,
# not encoded in the type.
type HandlerMap = dict[str, HandlerCallable]


@dataclass
class JobSpec:
    """Describes a job to be dispatched: what to call and routing info for the reply."""

    fn_name: str
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    timeout_ms: int | None = None
    reply_to: bytes | None = None
    sequence_number: int | None = None


@dataclass
class ResultItem:
    """A child job's result routed back to its suspended parent's worker."""

    sequence_number: int
    body: Any


# A unit of work handed to a worker: a job to run, or a buffered child result
type WorkItem = JobSpec | ResultItem


@dataclass
class LeaseTracker:
    """Lease id of the last work item this worker received; acked on the next get-job request.

    None until the first work item arrives.
    """

    ack: int | None = None


@dataclass
class SilenceTracker:
    """Accumulated reply-less park windows — detects a dead overseer on remote workers.

    max_silence_ms None means never raise; local workers are killed by the vm instead.
    """

    max_silence_ms: int | None = None
    silent_ms: int = 0
