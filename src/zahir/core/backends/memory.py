"""In-memory coordination backend — the default storage backend for the overseer gen_server."""

from collections import deque
from collections.abc import Generator, Sequence
from dataclasses import dataclass, field
from functools import partial
from typing import Any, cast

from zahir.core.combinators import build_handler_map
from zahir.core.constants import WorkItemTag
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
        """Return a buffered result for this worker, the next queued job, or
        None."""
        results = self.pending_results.get(worker_pid_bytes)
        if results:
            sequence_number, body = results.popleft()
            return (WorkItemTag.RESULT, sequence_number, body)

        if self.queue:
            job = self.queue.popleft()
            job_tuple = (
                WorkItemTag.JOB,
                job.fn_name,
                job.args,
                job.reply_to,
                job.timeout_ms,
                job.sequence_number,
            )
            return job_tuple

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
    return backend.get_job(effect.worker_pid_bytes)


def _handle_storage_enqueue(
    backend: MemoryBackend,
    effect: EStorageEnqueue,
) -> Generator[Any, Any, None]:
    """Add a child job to the queue."""
    backend.enqueue(
        JobSpec(
            fn_name=effect.fn_name,
            args=effect.args,
            reply_to=effect.reply_to,
            timeout_ms=effect.timeout_ms,
            sequence_number=effect.sequence_number,
        )
    )
    yield from ()
    return


def _handle_storage_job_done(
    backend: MemoryBackend,
    effect: EStorageJobDone,
) -> Generator[Any, Any, None]:
    """Record job completion and route the result."""
    backend.job_done(effect.reply_to, effect.sequence_number, effect.body)
    yield from ()
    return


def _handle_storage_job_failed(
    backend: MemoryBackend,
    effect: EStorageJobFailed,
) -> Generator[Any, Any, None]:
    """Record a root-level job failure."""
    backend.job_failed(effect.error)
    yield from ()
    return


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


# EStorageGetJob is polled on every worker tick — wrapping it floods telemetry with noise.
# Mirrors the EGetJob skip in coordination_handlers.
_SKIP_WRAP = frozenset({EStorageGetJob.tag})


def make_memory_storage_handlers(handler_wrappers: Sequence = ()) -> StorageHandlerMap:
    """Create a complete set of storage handlers backed by a fresh in-memory backend,
    with any user-supplied wrappers applied."""
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
    return cast(StorageHandlerMap, build_handler_map(bindings, handler_wrappers, skip=_SKIP_WRAP))
