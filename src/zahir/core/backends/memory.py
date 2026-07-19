"""In-memory coordination backend — the default storage backend for the overseer gen_server."""

from collections import deque
from collections.abc import Generator, Sequence
from dataclasses import dataclass, field
from functools import partial
from typing import Any, cast

from zahir.core.combinators import build_handler_map
from zahir.core.effects import (
    EStorageAcquire,
    EStorageEnqueue,
    EStorageGetError,
    EStorageGetJob,
    EStorageGetResult,
    EStorageGetState,
    EStorageIsDone,
    EStorageJobDone,
    EStorageJobFailed,
    EStorageRelease,
    EStorageSetState,
)
from zahir.core.zahir_types import (
    ConcurrencyMap,
    JobSpec,
    LeaseMap,
    PendingResults,
    ResultItem,
    StorageHandlerMap,
    WorkItem,
)


@dataclass
class MemoryBackend:
    """Stores all coordination state in-process. Requires no external dependencies.

    Safe without locking because all access is serialised through the overseer gen_server's
    single-threaded message loop.
    """

    queue: deque = field(default_factory=deque)
    concurrency: ConcurrencyMap = field(default_factory=dict)
    semaphores: dict = field(default_factory=dict)
    pending: int = 0
    root_error: Exception | None = None
    root_result: Any = None
    pending_results: PendingResults = field(default_factory=dict)
    leased: LeaseMap = field(default_factory=dict)
    lease_counter: int = 0

    def settle_lease(self, worker_pid_bytes: bytes, ack: int | None) -> Any:
        """Drop an acked lease; re-deliver an un-acked one. None when no lease is open."""

        lease = self.leased.get(worker_pid_bytes)
        if lease is None:
            return None

        lease_id, _work = lease
        if ack == lease_id:
            del self.leased[worker_pid_bytes]
            return None
        return lease

    def open_lease(self, worker_pid_bytes: bytes, work: Any) -> tuple[int, Any]:
        """Record a work handout so it survives a lost reply, and return the leased item."""

        self.lease_counter += 1
        lease = (self.lease_counter, work)
        self.leased[worker_pid_bytes] = lease
        return lease

    def next_work_item(self, worker_pid_bytes: bytes) -> WorkItem | None:
        """Return a buffered result for this worker, the next queued job, or None."""

        results = self.pending_results.get(worker_pid_bytes)
        if results:
            sequence_number, body = results.popleft()
            return ResultItem(sequence_number=sequence_number, body=body)

        if self.queue:
            return self.queue.popleft()

        return None

    def get_job(self, worker_pid_bytes: bytes, ack: int | None) -> Any:
        """Return the worker's un-acked lease, a buffered result, or the next queued job.

        Every work handout is leased: the reply can be lost when it crosses the
        worker's heartbeat timeout, so the item is held until the worker's next
        request acks receipt. Returns (lease_id, work) or None.
        """

        redelivery = self.settle_lease(worker_pid_bytes, ack)
        if redelivery is not None:
            return redelivery

        work = self.next_work_item(worker_pid_bytes)
        if work is not None:
            return self.open_lease(worker_pid_bytes, work)

        return None

    def enqueue(self, spec: JobSpec) -> None:
        """Add a child job to the queue and increment pending."""
        self.queue.append(spec)
        self.pending += 1

    def job_done(
        self,
        reply_to_bytes: bytes | None,
        sequence_number: Any,
        body: Any,
    ) -> None:
        """Decrement pending and buffer result for waiting parent, or store as
        root result."""
        self.pending -= 1
        if reply_to_bytes is None:
            self.root_result = body
        else:
            if reply_to_bytes not in self.pending_results:
                self.pending_results[reply_to_bytes] = deque()
            self.pending_results[reply_to_bytes].append((sequence_number, body))

    def job_failed(self, error: Exception) -> None:
        """Decrement pending and record the first root error."""
        self.pending -= 1
        if self.root_error is None:
            self.root_error = error

    def acquire(self, name: str, limit: int) -> bool:
        """Try to acquire a concurrency slot. Returns True if granted."""
        current_limit, count = self.concurrency.get(name, (limit, 0))
        if count < current_limit:
            self.concurrency[name] = (current_limit, count + 1)
            return True
        return False

    def release(self, name: str) -> None:
        """Release a concurrency slot, clamping at zero."""
        if name in self.concurrency:
            current_limit, count = self.concurrency[name]
            self.concurrency[name] = (current_limit, max(0, count - 1))

    def signal(self, name: str) -> str | None:
        """Return the current semaphore state, or None if unset."""
        return self.semaphores.get(name)

    def set_semaphore(self, name: str, state: str) -> None:
        """Set the semaphore state for the given name."""
        self.semaphores[name] = state

    def is_done(self) -> bool:
        """Return True when pending is zero and the queue is empty."""
        return self.pending == 0 and not self.queue

    def get_error(self) -> Exception | None:
        """Return the root error, if any."""
        return self.root_error

    def get_result(self) -> Any:
        """Return the root job's return value."""
        return self.root_result


# Storage effect handler functions — each takes a MemoryBackend instance and a storage effect.


def _handle_storage_get_job(backend: MemoryBackend, effect: EStorageGetJob) -> Any:
    """Return the next work item for the requesting worker."""
    yield from ()
    return backend.get_job(effect.worker_pid_bytes, effect.ack)


def _handle_storage_enqueue(
    backend: MemoryBackend,
    effect: EStorageEnqueue,
) -> Generator[Any, Any, None]:
    """Add a child job to the queue."""
    backend.enqueue(effect.job)
    yield from ()
    return


def _handle_storage_job_done(
    backend: MemoryBackend,
    effect: EStorageJobDone,
) -> Generator[Any, Any, bool]:
    """Record job completion and route the result. Returns True when the workflow is done,
    so the overseer can wake completion waiters without a second dispatch."""
    backend.job_done(effect.reply_to, effect.sequence_number, effect.body)
    yield from ()
    return backend.is_done()


def _handle_storage_job_failed(
    backend: MemoryBackend,
    effect: EStorageJobFailed,
) -> Generator[Any, Any, bool]:
    """Record a root-level job failure. Returns True when the workflow is done,
    so the overseer can wake completion waiters without a second dispatch."""
    backend.job_failed(effect.error)
    yield from ()
    return backend.is_done()


def _handle_storage_acquire(
    backend: MemoryBackend,
    effect: EStorageAcquire,
) -> Generator[Any, Any, bool]:
    """Try to acquire a concurrency slot."""
    yield from ()
    return backend.acquire(effect.name, effect.limit)


def _handle_storage_release(
    backend: MemoryBackend,
    effect: EStorageRelease,
) -> Generator[Any, Any, None]:
    """Release a concurrency slot."""
    backend.release(effect.name)
    yield from ()
    return


def _handle_storage_get_state(backend: MemoryBackend, effect: EStorageGetState) -> Any:
    """Return the current KV state."""
    yield from ()
    return backend.signal(effect.name)


def _handle_storage_set_state(
    backend: MemoryBackend,
    effect: EStorageSetState,
) -> Generator[Any, Any, None]:
    """Set the KV state."""
    backend.set_semaphore(effect.name, effect.state)
    yield from ()
    return


def _handle_storage_is_done(
    backend: MemoryBackend,
    effect: EStorageIsDone,
) -> Generator[Any, Any, bool]:
    """Return True when all jobs have completed."""
    yield from ()
    return backend.is_done()


def _handle_storage_get_error(backend: MemoryBackend, effect: EStorageGetError) -> Any:
    """Return the root error, if any."""
    yield from ()
    return backend.get_error()


def _handle_storage_get_result(backend: MemoryBackend, effect: EStorageGetResult) -> Any:
    """Return the root job's return value."""
    yield from ()
    return backend.get_result()


def make_memory_storage_handlers(handler_wrappers: Sequence = ()) -> StorageHandlerMap:
    """Create a complete set of storage handlers backed by a fresh in-memory backend,
    with any user-supplied wrappers applied.

    EStorageGetJob is wrapped like everything else: workers long-poll rather than
    spin, so get-job telemetry is meaningful signal, not per-tick noise.
    """
    backend = MemoryBackend()

    bindings = {
        EStorageGetJob.tag: partial(_handle_storage_get_job, backend),
        EStorageEnqueue.tag: partial(_handle_storage_enqueue, backend),
        EStorageJobDone.tag: partial(_handle_storage_job_done, backend),
        EStorageJobFailed.tag: partial(_handle_storage_job_failed, backend),
        EStorageAcquire.tag: partial(_handle_storage_acquire, backend),
        EStorageRelease.tag: partial(_handle_storage_release, backend),
        EStorageGetState.tag: partial(_handle_storage_get_state, backend),
        EStorageSetState.tag: partial(_handle_storage_set_state, backend),
        EStorageIsDone.tag: partial(_handle_storage_is_done, backend),
        EStorageGetError.tag: partial(_handle_storage_get_error, backend),
        EStorageGetResult.tag: partial(_handle_storage_get_result, backend),
    }
    return cast(StorageHandlerMap, build_handler_map(bindings, handler_wrappers))
