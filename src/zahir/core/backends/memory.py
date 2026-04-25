# In-memory coordination backend — the default backend for the overseer gen_server.
from collections import deque
from typing import Any

from zahir.core.constants import WorkItemTag
from zahir.core.zahir_types import ConcurrencyMap, JobSpec, PendingResults


class MemoryBackend:
    """Stores all coordination state in-process. Requires no external dependencies.

    Safe without locking because all access is serialised through the overseer gen_server's
    single-threaded message loop.
    """

    def __init__(
        self,
        queue: deque | None = None,
        concurrency: ConcurrencyMap | None = None,
        semaphores: dict | None = None,
        pending: int = 0,
        root_error: Exception | None = None,
        root_result: Any = None,
        pending_results: PendingResults | None = None,
    ) -> None:
        self.queue = queue if queue is not None else deque()
        self.concurrency = concurrency if concurrency is not None else {}
        self.semaphores = semaphores if semaphores is not None else {}
        self.pending = pending
        self.root_error = root_error
        self.root_result = root_result
        self.pending_results = pending_results if pending_results is not None else {}

    def initialize(self, fn_name: str, args: tuple) -> None:
        """Seed the queue with the root job and set pending to 1."""
        self.queue.append(JobSpec(fn_name=fn_name, args=args, reply_to=None))
        self.pending = 1

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

    def enqueue(self, fn_name: str, args: tuple, reply_to: bytes | None, timeout_ms: int | None, sequence_number: int | None) -> None:
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

    def set_semaphore(self, name: str, sem_state: str) -> None:
        """Set the semaphore state for the given name."""
        self.semaphores[name] = sem_state

    def is_done(self) -> bool:
        """Return True when pending is zero and the queue is empty."""
        return self.pending == 0 and not self.queue

    def get_error(self) -> Exception | None:
        """Return the root error, if any."""
        return self.root_error

    def get_result(self) -> Any:
        """Return the root job's return value."""
        return self.root_result
