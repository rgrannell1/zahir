"""Effects yielded by the overseer gen_server — handled by storage backends."""

from dataclasses import dataclass
from typing import Any, ClassVar, LiteralString

from orbis import Effect

from zahir.core.zahir_types import JobSpec


class ZahirStorageEffect[ReturnT](Effect[ReturnT], abstract=True):
    """Base class for storage effects yielded by the overseer gen_server.

    These effects are handled by the storage backend.
    """


@dataclass
class EStorageGetJob(ZahirStorageEffect[Any]):
    """Return the next work item for a worker from the backend.

    ack carries the lease id of the last work item the worker received, confirming
    delivery so the backend can drop its copy and hand out the next item.
    """

    tag: ClassVar[LiteralString] = "storage_get_job"
    worker_pid_bytes: bytes
    ack: int | None = None


@dataclass
class EStorageEnqueue(ZahirStorageEffect[None]):
    """Enqueue a child job and increment pending."""

    tag: ClassVar[LiteralString] = "storage_enqueue"
    job: JobSpec


@dataclass
class EStorageJobDone(ZahirStorageEffect[None]):
    """Decrement pending and route a result or error to the parent worker.

    If there is no parent, stores as root result.
    """

    tag: ClassVar[LiteralString] = "storage_job_done"
    reply_to: bytes | None
    sequence_number: Any
    body: Any


@dataclass
class EStorageJobFailed(ZahirStorageEffect[None]):
    """Decrement pending and record a root-level failure."""

    tag: ClassVar[LiteralString] = "storage_job_failed"
    error: Exception


@dataclass
class EStorageAcquire(ZahirStorageEffect[bool]):
    """Try to acquire a named concurrency slot."""

    tag: ClassVar[LiteralString] = "storage_acquire"
    name: str
    limit: int


@dataclass
class EStorageRelease(ZahirStorageEffect[None]):
    """Release a named concurrency slot."""

    tag: ClassVar[LiteralString] = "storage_release"
    name: str


@dataclass
class EStorageGetState(ZahirStorageEffect[Any]):
    """Return the current KV state for a name."""

    tag: ClassVar[LiteralString] = "storage_get_state"
    name: str


@dataclass
class EStorageSetState(ZahirStorageEffect[None]):
    """Set the KV state for a name."""

    tag: ClassVar[LiteralString] = "storage_set_state"
    name: str
    state: str


@dataclass
class EStorageIsDone(ZahirStorageEffect[bool]):
    """Return True when all pending jobs have completed."""

    tag: ClassVar[LiteralString] = "storage_is_done"


@dataclass
class EStorageGetError(ZahirStorageEffect[Any]):
    """Return the root error, if any."""

    tag: ClassVar[LiteralString] = "storage_get_error"


@dataclass
class EStorageGetResult(ZahirStorageEffect[Any]):
    """Return the root job's return value."""

    tag: ClassVar[LiteralString] = "storage_get_result"
