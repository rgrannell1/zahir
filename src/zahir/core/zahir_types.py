from collections import deque
from dataclasses import dataclass, field
from typing import Any, Literal

# Result types for dependency combinators — the Left/Right of the dependency monad.
type Satisfied = tuple[Literal["satisfied"], dict | None]
type Impossible = tuple[Literal["impossible"], str]
type DependencyResult = Satisfied | Impossible


@dataclass
class JobSpec:
    """Describes a job to be dispatched: what to call and routing info for the reply."""

    fn_name: str
    args: tuple[Any, ...] = field(default_factory=tuple)
    timeout_ms: int | None = None
    reply_to: bytes | None = None
    nonce: int | None = None


@dataclass
class OverseerState:
    queue: deque[JobSpec]
    concurrency: dict[str, tuple[int, int]]  # name -> (limit, active_count)
    semaphores: dict[str, str]  # name -> 'satisfied'|'unsatisfied'|'impossible'
    pending: int
    root_error: Exception | None = None
    pending_results: dict[bytes, deque] = field(
        default_factory=dict
    )  # worker_pid_bytes -> deque[(nonce, body)]
