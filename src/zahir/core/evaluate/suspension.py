"""Fan-out suspension table.

Tracks jobs waiting on child results and resumes them when all children
complete.
"""

import itertools
from collections.abc import Generator
from dataclasses import dataclass, field, replace
from typing import Any

from zahir.core.commons.fp_types import Err, Ok, Result
from zahir.core.commons.zahir_types import ResultItem
from zahir.core.effects import EAwait, EEnqueue
from zahir.core.exceptions import JobError, JobTimeoutError

# Return type of SuspensionTable.resume. None when children are still outstanding
type ResumeResult = tuple[RunningJob, Result[Any, Exception]] | None


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

    # monotonic-clock instant after which this job has timed out; None means no timeout
    deadline: float | None = None


@dataclass
class WorkerLocals:
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

    # gather dispatch: resume with a list of Ok/Err rather than throwing the first error
    gather: bool = False

    @classmethod
    def from_job(
        cls, job: RunningJob, child_sequence_numbers: list[int], effect: EAwait
    ) -> "SuspendedJob":
        """Promote a running job to suspended, recording the fan-out from an EAwait."""

        # we only care about ordering for multi-job dispatch
        result_order = None if effect.scalar else child_sequence_numbers
        return cls(
            fn_name=job.fn_name,
            eval_gen=job.eval_gen,
            reply_to=job.reply_to,
            parent_sequence_number=job.parent_sequence_number,
            acquired=job.acquired,
            deadline=job.deadline,
            awaiting=set(child_sequence_numbers),
            results={},
            result_order=result_order,
            gather=effect.gather,
        )


def _unwrap_reply(body: Any) -> Result[Any, Exception]:
    """Unwrap the reply from a job, returning Ok(value) or Err(error)."""

    if isinstance(body, (JobTimeoutError, JobError)):
        return Err(body)

    return Ok(body)


def _collect_gather(job: SuspendedJob) -> Result[list, Exception]:
    """Collect ordered multi-job results as a list of Ok/Err; never a top-level Err."""

    assert job.result_order is not None

    gathered = [_unwrap_reply(job.results[seq]) for seq in job.result_order]
    return Ok(gathered)


def _collect_result(job: SuspendedJob, body: Any) -> Result[Any, Exception]:
    """Choose the collection strategy matching the completed fan-out's dispatch mode."""

    if job.gather:
        return _collect_gather(job)
    if job.result_order is not None:
        return _collect_await_many(job)
    return _unwrap_reply(body)


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

    def suspend(
        self, effect: EAwait, job: RunningJob, me_bytes: bytes
    ) -> Generator[Any, Any, None]:
        """Suspend job, enqueue all child jobs, and record the fan-out in the table.

        A failed enqueue mid-fan-out unregisters the whole fan-out before re-raising,
        so results from children that did enqueue are discarded by resume rather than
        crashing the worker on a missing parent.
        """

        child_sequence_numbers = [self._alloc() for _ in effect.jobs]
        parent_key = self._alloc()

        try:
            for cn, spec in zip(child_sequence_numbers, effect.jobs):
                self.child_to_parent[cn] = parent_key
                yield EEnqueue(job=replace(spec, reply_to=me_bytes, sequence_number=cn))
        except BaseException:
            for cn in child_sequence_numbers:
                self.child_to_parent.pop(cn, None)
            raise

        self.waiting[parent_key] = SuspendedJob.from_job(job, child_sequence_numbers, effect)

    def resume(self, work: ResultItem) -> ResumeResult:
        """Record a child result.

        Returns (job, result) when all children are done, None if still
        waiting. Results for unregistered children — e.g. late arrivals for an
        expired parent — are discarded.
        """

        parent_key = self.child_to_parent.pop(work.sequence_number, None)
        if parent_key is None:
            return None

        job = self.waiting[parent_key]
        job.results[work.sequence_number] = work.body
        job.awaiting.remove(work.sequence_number)

        if job.awaiting:
            return None

        current = self.waiting.pop(parent_key)
        return current, _collect_result(current, work.body)

    def pop_expired(self, now: float) -> SuspendedJob | None:
        """Remove and return one suspended job whose deadline has passed, or None.

        The expired job's outstanding children are unregistered so their late
        results are discarded by resume.
        """

        expired_key = next(
            (
                parent_key
                for parent_key, job in self.waiting.items()
                if job.deadline is not None and now >= job.deadline
            ),
            None,
        )
        if expired_key is None:
            return None

        job = self.waiting.pop(expired_key)
        for child_sequence_number in job.awaiting:
            self.child_to_parent.pop(child_sequence_number, None)
        return job
