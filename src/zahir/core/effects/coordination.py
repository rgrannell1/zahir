"""Effects yielded by the worker/runtime layer — never by user job code directly."""

from dataclasses import dataclass
from typing import Any, ClassVar

from orbis import Effect


class ZahirCoordinationEffect[ReturnT](Effect[ReturnT], abstract=True):
    """Base class for effects yielded by the runtime layer, never by jobs directly."""


@dataclass
class EEnqueue(ZahirCoordinationEffect[None]):
    """Internal: enqueue a child job and route its reply back to this worker."""

    tag: ClassVar[str] = "enqueue"
    fn_name: str
    args: tuple[Any, ...]
    reply_to: bytes | None  # the requesting worker's PID bytes; None for the root job
    timeout_ms: int | None
    sequence_number: int | None  # allocated sequence_number for routing the reply back to the parent; None for the root job


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
    sequence_number: Any
    fn_name: str = ""  # TODO: deprecation candidate — piggybacks fn_name for telemetry; job identity should come from a dedicated event  # noqa: E501


@dataclass
class EJobFail(ZahirCoordinationEffect[None]):
    """Internal: report job failure (timeout or error) and route the exception to the caller."""

    tag: ClassVar[str] = "job_fail"
    error: Exception
    reply_to: bytes | None
    sequence_number: Any
    fn_name: str = ""  # TODO: deprecation candidate — piggybacks fn_name for telemetry; job identity should come from a dedicated event  # noqa: E501


@dataclass
class EIsDone(ZahirCoordinationEffect[bool]):
    """Internal: ask the overseer whether all pending jobs have completed."""

    tag: ClassVar[str] = "is_done"


@dataclass
class EGetError(ZahirCoordinationEffect[Exception | None]):
    """Internal: retrieve the root error from the overseer, if any job failed fatally."""

    tag: ClassVar[str] = "get_error"


@dataclass
class EGetResult(ZahirCoordinationEffect[Any]):
    """Internal: retrieve the root job's return value from the overseer."""

    tag: ClassVar[str] = "get_result"
