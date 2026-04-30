"""Fan-out suspension table: tracks jobs waiting on child results and resumes them when all children complete."""

import itertools
from collections.abc import Generator
from dataclasses import dataclass, field
from typing import Any

from zahir.core.effects import EAwait, EEnqueue
from zahir.core.exceptions import JobError, JobTimeoutError
from zahir.core.fp_types import Err, Ok, Result


@dataclass
class RunningJob:
    """A job currently being stepped through by the worker."""

    # the name of the function being executed
    fn_name: str

    # the evaluate_job generator
    eval_gen: Any

    # where to send our result when we finish
    reply_to: bytes | None

    # the sequence_number our parent assigned us
    parent_sequence_number: Any

    # concurrency slots held by this job
    acquired: list[str] = field(default_factory=list)


@dataclass
class _WorkerLocals:
    """Mutable per-worker references written by the worker loop and read by the EAwait handler."""

    me_bytes: bytes = b""
    current_job: RunningJob | None = None


@dataclass
class SuspendedJob(RunningJob):
    """A running job that has yielded EAwait and is waiting on one or more child jobs."""

    # child local sequence_numbers still outstanding
    awaiting: set[int] = field(default_factory=set)

    # local sequence_number -> body (result or error)
    results: dict[int, Any] = field(default_factory=dict)

    # None for scalar dispatch; ordered sequence_number list for multi-job dispatch
    result_order: list[int] | None = None

    @classmethod
    def from_job(cls, job: RunningJob, child_sequence_numbers: list[int], effect: EAwait) -> "SuspendedJob":
        """Promote a running job to suspended, recording the fan-out from an EAwait."""

        # we only care about ordering for multi-job dispatch
        result_order = None if effect.scalar else child_sequence_numbers
        return cls(
            fn_name=job.fn_name,
            eval_gen=job.eval_gen,
            reply_to=job.reply_to,
            parent_sequence_number=job.parent_sequence_number,
            acquired=job.acquired,
            awaiting=set(child_sequence_numbers),
            results={},
            result_order=result_order,
        )


# (tag, sequence_number, body) — the raw work item delivered to a waiting parent
type WorkItem = tuple[str, int | None, Any]

# Return type of SuspensionTable.resume — None when children are still outstanding
type ResumeResult = tuple[RunningJob, Result[Any, Exception]] | None


def _unwrap_reply(body: Any) -> Result[Any, Exception]:
    """Unwrap the reply from a job, returning Ok(value) or Err(error)."""

    if isinstance(body, (JobTimeoutError, JobError)):
        return Err(body)

    return Ok(body)


def _collect_await_many(job: SuspendedJob) -> Result[list, Exception]:
    """Collect ordered multi-job results, surfacing the first error if any."""

    assert job.result_order is not None

    results = []
    first_error: Exception | None = None

    for sequence_number in job.result_order:
        match _unwrap_reply(job.results[sequence_number]):
            case Ok(value):
                results.append(value)
            case Err(error) if first_error is None:
                first_error = error

    if first_error is not None:
        return Err(first_error)

    return Ok(results)


@dataclass
class SuspensionTable:
    """Tracks jobs suspended on EAwait, mapping child sequence numbers back to their parent."""

    waiting: dict[int, SuspendedJob] = field(default_factory=dict)
    child_to_parent: dict[int, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self._alloc = itertools.count().__next__

    def suspend(self, effect: EAwait, job: RunningJob, me_bytes: bytes) -> Generator[Any, Any, None]:
        """Suspend job, enqueue all child jobs, and record the fan-out in the table."""

        child_sequence_numbers = [self._alloc() for _ in effect.jobs]
        parent_key = self._alloc()

        for cn, spec in zip(child_sequence_numbers, effect.jobs):
            self.child_to_parent[cn] = parent_key
            yield EEnqueue(
                fn_name=spec.fn_name,
                args=spec.args,
                reply_to=me_bytes,
                timeout_ms=spec.timeout_ms,
                sequence_number=cn,
            )

        self.waiting[parent_key] = SuspendedJob.from_job(job, child_sequence_numbers, effect)

    def resume(self, work: WorkItem) -> ResumeResult:
        """Record a child result. Returns (job, result) when all children are done, None if still waiting."""

        _, child_sequence_number, body = work

        assert child_sequence_number is not None

        parent_key = self.child_to_parent.pop(child_sequence_number)
        job = self.waiting[parent_key]
        job.results[child_sequence_number] = body
        job.awaiting.remove(child_sequence_number)

        if job.awaiting:
            return None

        current = self.waiting.pop(parent_key)
        result = _collect_await_many(current) if current.result_order is not None else _unwrap_reply(body)
        return current, result
