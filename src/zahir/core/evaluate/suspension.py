"""Fan-out suspension table: tracks jobs waiting on child results and resumes them when all children complete."""

import itertools
from collections.abc import Generator
from dataclasses import dataclass, field
from typing import Any

from zahir.core.effects import EAwait, EEnqueue
from zahir.core.evaluate.job_handlers import JobHandlerContext, _unwrap_reply
from zahir.core.exceptions import JobError, JobTimeout


@dataclass
class _RunningJob:
    """A job currently being stepped through by the worker."""

    fn_name: str  # the name of the function being executed
    eval_gen: Any  # the evaluate_job generator
    context: JobHandlerContext
    reply_to: bytes | None  # where to send our result when we finish
    parent_sequence_number: Any  # the sequence_number our parent assigned us


@dataclass
class _SuspendedJob(_RunningJob):
    """A running job that has yielded EAwait and is waiting on one or more child jobs."""

    awaiting: set[int]  # child local sequence_numbers still outstanding
    results: dict[int, Any]  # local sequence_number -> body (result or error)
    result_order: (
        list[int] | None
    )  # None for scalar dispatch; ordered sequence_number list for multi-job dispatch


def _collect_scalar_result(body: Any) -> tuple[Any, Exception | None]:
    """Unwrap a single child result into (value, error)."""

    try:
        return _unwrap_reply(body), None
    except (JobError, JobTimeout) as exc:
        return None, exc


def _collect_multi_results(job: _SuspendedJob) -> tuple[list | None, Exception | None]:
    """Collect ordered multi-job results, surfacing the first error if any."""

    first_error: Exception | None = None
    results = []
    for sequence_number in job.result_order:
        try:
            results.append(_unwrap_reply(job.results[sequence_number]))
        except (JobError, JobTimeout) as exc:
            if first_error is None:
                first_error = exc

    if first_error is not None:
        return None, first_error

    return results, None


@dataclass
class _SuspensionTable:
    """Tracks jobs suspended on EAwait, mapping child sequence numbers back to their parent."""

    waiting: dict[int, _SuspendedJob] = field(default_factory=dict)
    child_to_parent: dict[int, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self._alloc = itertools.count().__next__

    def suspend(
        self, effect: EAwait, job: _RunningJob, me_bytes: bytes
    ) -> Generator[Any, Any, None]:
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

        self.waiting[parent_key] = _SuspendedJob(
            fn_name=job.fn_name,
            eval_gen=job.eval_gen,
            context=job.context,
            reply_to=job.reply_to,
            parent_sequence_number=job.parent_sequence_number,
            awaiting=set(child_sequence_numbers),
            results={},
            result_order=None if effect.scalar else child_sequence_numbers,
        )

    def resume(self, work: tuple) -> tuple[_RunningJob, Any, Exception | None] | None:
        """Record a child result. Returns (job, handler_value, pending_throw) when all children are done, None if still waiting."""

        _, child_sequence_number, body = work
        parent_key = self.child_to_parent.pop(child_sequence_number)
        job = self.waiting[parent_key]
        job.results[child_sequence_number] = body
        job.awaiting.remove(child_sequence_number)

        if job.awaiting:
            return None

        current = self.waiting.pop(parent_key)
        if current.result_order is not None:
            handler_value, pending_throw = _collect_multi_results(current)
        else:
            handler_value, pending_throw = _collect_scalar_result(body)
        return current, handler_value, pending_throw
