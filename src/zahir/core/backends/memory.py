"""In-memory coordination backend — the default storage backend for the overseer gen_server."""

from collections import deque
from dataclasses import dataclass, field
from functools import partial
from typing import Any

from zahir.core.constants import WorkItemTag
from zahir.core.effects import (
    EStorageAcquire,
    EStorageEnqueue,
    EStorageGetError,
    EStorageGetJob,
    EStorageGetResult,
    EStorageIsDone,
    EStorageJobDone,
    EStorageJobFailed,
    EStorageRelease,
    EStorageSetSemaphore,
    EStorageSignal,
)
from zahir.core.zahir_types import ConcurrencyMap, JobSpec, PendingResults, StorageHandlerMap


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

    def get_job(self, worker_pid_bytes: bytes) -> Any:
        """Return a buffered result for this worker, the next queued job, or None."""
        results = self.pending_results.get(worker_pid_bytes)
        if results:
            sequence_number, body = results.popleft()
            return (WorkItemTag.RESULT, sequence_number, body)

        if self.queue:
            job = self.queue.popleft()
            return (WorkItemTag.JOB, job.fn_name, job.args, job.reply_to, job.timeout_ms, job.sequence_number)

        return None

    def enqueue(
        self, fn_name: str, args: tuple, reply_to: bytes | None, timeout_ms: int | None, sequence_number: int | None
    ) -> None:  # noqa: E501
        """Add a child job to the queue and increment pending."""
        spec = JobSpec(fn_name=fn_name, args=args, reply_to=reply_to, timeout_ms=timeout_ms, sequence_number=sequence_number)
        self.queue.append(spec)
        self.pending += 1

    def job_done(self, reply_to_bytes: bytes | None, sequence_number: Any, body: Any) -> None:
        """Decrement pending and buffer the result for the waiting parent, or store as root result."""
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
    return backend.get_job(effect.worker_pid_bytes)
    yield


def _handle_storage_enqueue(backend: MemoryBackend, effect: EStorageEnqueue) -> None:
    """Add a child job to the queue."""
    backend.enqueue(effect.fn_name, effect.args, effect.reply_to, effect.timeout_ms, effect.sequence_number)
    return
    yield


def _handle_storage_job_done(backend: MemoryBackend, effect: EStorageJobDone) -> None:
    """Record job completion and route the result."""
    backend.job_done(effect.reply_to, effect.sequence_number, effect.body)
    return
    yield


def _handle_storage_job_failed(backend: MemoryBackend, effect: EStorageJobFailed) -> None:
    """Record a root-level job failure."""
    backend.job_failed(effect.error)
    return
    yield


def _handle_storage_acquire(backend: MemoryBackend, effect: EStorageAcquire) -> bool:
    """Try to acquire a concurrency slot."""
    return backend.acquire(effect.name, effect.limit)
    yield


def _handle_storage_release(backend: MemoryBackend, effect: EStorageRelease) -> None:
    """Release a concurrency slot."""
    backend.release(effect.name)
    return
    yield


def _handle_storage_signal(backend: MemoryBackend, effect: EStorageSignal) -> Any:
    """Return the current semaphore state."""
    return backend.signal(effect.name)
    yield


def _handle_storage_set_semaphore(backend: MemoryBackend, effect: EStorageSetSemaphore) -> None:
    """Set the semaphore state."""
    backend.set_semaphore(effect.name, effect.state)
    return
    yield


def _handle_storage_is_done(backend: MemoryBackend, effect: EStorageIsDone) -> bool:
    """Return True when all jobs have completed."""
    return backend.is_done()
    yield


def _handle_storage_get_error(backend: MemoryBackend, effect: EStorageGetError) -> Any:
    """Return the root error, if any."""
    return backend.get_error()
    yield


def _handle_storage_get_result(backend: MemoryBackend, effect: EStorageGetResult) -> Any:
    """Return the root job's return value."""
    return backend.get_result()
    yield


def make_memory_storage_handlers() -> StorageHandlerMap:
    """Create a complete set of storage handlers backed by a fresh in-memory backend."""
    backend = MemoryBackend()
    return {
        EStorageGetJob.tag: partial(_handle_storage_get_job, backend),
        EStorageEnqueue.tag: partial(_handle_storage_enqueue, backend),
        EStorageJobDone.tag: partial(_handle_storage_job_done, backend),
        EStorageJobFailed.tag: partial(_handle_storage_job_failed, backend),
        EStorageAcquire.tag: partial(_handle_storage_acquire, backend),
        EStorageRelease.tag: partial(_handle_storage_release, backend),
        EStorageSignal.tag: partial(_handle_storage_signal, backend),
        EStorageSetSemaphore.tag: partial(_handle_storage_set_semaphore, backend),
        EStorageIsDone.tag: partial(_handle_storage_is_done, backend),
        EStorageGetError.tag: partial(_handle_storage_get_error, backend),
        EStorageGetResult.tag: partial(_handle_storage_get_result, backend),
    }
