from collections import deque
from dataclasses import dataclass


@dataclass
class JobSpec:
    fn_name: str
    args: tuple
    reply_to: bytes | None
    timeout_ms: int | None = None
    nonce: int | None = None




@dataclass
class OverseerState:
    queue: deque
    concurrency: dict[str, tuple[int, int]]  # name -> (limit, active_count)
    semaphores: dict[str, str]               # name -> 'satisfied'|'unsatisfied'|'impossible'
    pending: int
