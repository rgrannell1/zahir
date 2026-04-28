"""Effects yielded by the overseer gen_server — handled by storage backends."""

from dataclasses import dataclass
from typing import Any, ClassVar

from orbis import Effect


class ZahirStorageEffect[ReturnT](Effect[ReturnT], abstract=True):
    """Base class for storage effects yielded by the overseer gen_server — handled by the storage backend."""


@dataclass
class EStorageInitialize(ZahirStorageEffect[None]):
    """Seed the backend with the root job on overseer startup."""

    tag: ClassVar[str] = "storage:initialize"
    fn_name: str
    args: tuple[Any, ...]


@dataclass
class EStorageGetJob(ZahirStorageEffect[Any]):
    """Return the next work item for a worker from the backend."""

    tag: ClassVar[str] = "storage:get_job"
    worker_pid_bytes: bytes


@dataclass
class EStorageEnqueue(ZahirStorageEffect[None]):
    """Enqueue a child job and increment pending."""

    tag: ClassVar[str] = "storage:enqueue"
    fn_name: str
    args: tuple[Any, ...]
    reply_to: bytes | None
    timeout_ms: int | None
    sequence_number: int | None


@dataclass
class EStorageJobDone(ZahirStorageEffect[None]):
    """Decrement pending and route a result or error to the parent worker, or store as root result."""

    tag: ClassVar[str] = "storage:job_done"
    reply_to: bytes | None
    sequence_number: Any
    body: Any


@dataclass
class EStorageJobFailed(ZahirStorageEffect[None]):
    """Decrement pending and record a root-level failure."""

    tag: ClassVar[str] = "storage:job_failed"
    error: Exception


@dataclass
class EStorageAcquire(ZahirStorageEffect[bool]):
    """Try to acquire a named concurrency slot."""

    tag: ClassVar[str] = "storage:acquire"
    name: str
    limit: int


@dataclass
class EStorageRelease(ZahirStorageEffect[None]):
    """Release a named concurrency slot."""

    tag: ClassVar[str] = "storage:release"
    name: str


@dataclass
class EStorageSignal(ZahirStorageEffect[Any]):
    """Return the current semaphore state for a name."""

    tag: ClassVar[str] = "storage:signal"
    name: str


@dataclass
class EStorageSetSemaphore(ZahirStorageEffect[None]):
    """Set the semaphore state for a name."""

    tag: ClassVar[str] = "storage:set_semaphore"
    name: str
    state: str


@dataclass
class EStorageIsDone(ZahirStorageEffect[bool]):
    """Return True when all pending jobs have completed."""

    tag: ClassVar[str] = "storage:is_done"


@dataclass
class EStorageGetError(ZahirStorageEffect[Any]):
    """Return the root error, if any."""

    tag: ClassVar[str] = "storage:get_error"


@dataclass
class EStorageGetResult(ZahirStorageEffect[Any]):
    """Return the root job's return value."""

    tag: ClassVar[str] = "storage:get_result"
