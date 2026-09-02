# Helpers that translate zahir effects into bookman Event objects
import os
import time
import uuid
from dataclasses import dataclass

from bookman.bookman_types import Message
from bookman.events import Dims, Event, point, span
from orbis import Coeffect

from zahir.core.commons.constants import DependencyTag, JobTag, OperationKind, ParkTag, Phase
from zahir.core.effects import (
    EAwait,
    EEnqueue,
    EJobComplete,
    EJobFail,
    EStorageEnqueue,
    EStorageJobDone,
)


@dataclass
class TimeSpan:
    """A closed time interval [start, end] in seconds since epoch."""

    start: float
    end: float


def format_job_id(reply_to: bytes, sequence_number: int) -> str:
    """Form the globally unique job identifier used across telemetry."""

    return f"{reply_to.hex()}:{sequence_number}"


def get_fn_name(effect) -> str | None:
    """Get the function name from the effect, looking inside a carried JobSpec.

    Matching on the effect classes keeps this checked: a renamed field fails
    typing rather than silently degrading telemetry.
    """

    match effect:
        case EAwait(jobs=[spec], scalar=True):
            return spec.fn_name
        case EEnqueue(job=spec) | EStorageEnqueue(job=spec):
            return spec.fn_name
        case EJobComplete(fn_name=fn_name) | EJobFail(fn_name=fn_name):
            return fn_name or None
        case _:
            return None


def job_ref(reply_to: bytes | None, sequence_number: int | None, is_named: bool) -> str | None:
    """Job id from routing fields: formatted for a child, "root" when unrouted but named."""

    if sequence_number is not None and isinstance(reply_to, bytes):
        return format_job_id(reply_to, sequence_number)
    if sequence_number is None and reply_to is None and is_named:
        return "root"
    return None


def get_job_id(effect) -> str | None:
    """Form a globally unique job identifier from the sequence_number and reply_to.

    Enqueue effects carry these on their JobSpec; completion effects carry them directly.
    """

    match effect:
        case EEnqueue(job=spec) | EStorageEnqueue(job=spec):
            return job_ref(spec.reply_to, spec.sequence_number, is_named=True)
        case EJobComplete(reply_to=reply_to, sequence_number=seq):
            return job_ref(reply_to, seq, is_named=True)
        case EJobFail(reply_to=reply_to, sequence_number=seq):
            return job_ref(reply_to, seq, is_named=True)
        case EStorageJobDone(reply_to=reply_to, sequence_number=seq):
            return job_ref(reply_to, seq, is_named=False)
        case _:
            return None


def base_dimensions(operation, span_id: str) -> Dims:
    """We can filter all events by these dimensions."""

    pid = str(os.getpid())
    operation_kind = (
        OperationKind.COEFFECT if isinstance(operation, Coeffect) else OperationKind.EFFECT
    )
    dims: Dims = {
        "id": [span_id],
        "operation_kind": [operation_kind.value],
        "tag": [operation.tag],
        "pid": [pid],
    }

    fn = get_fn_name(operation)
    if fn:
        dims["fn"] = [fn]

    job_id = get_job_id(operation)
    if job_id:
        dims["job_id"] = [job_id]

    return dims


def start_effect_telemetry(effect, span_id: str, at: float) -> Event:
    """Point event marking when a handler began."""

    dims = base_dimensions(effect, span_id) | {"phase": [Phase.START.value]}
    return point(dims, at=at)


def end_effect_success_telemetry(
    effect, span_id: str, tspan: TimeSpan, value: Message | None = None
) -> Event:
    """Span event marking successful handler completion."""

    dims = base_dimensions(effect, span_id) | {"phase": [Phase.END.value]}
    return span(dims, at=tspan.start, until=tspan.end, value=value)


def end_effect_error_telemetry(effect, span_id: str, tspan: TimeSpan, error: str) -> Event:
    """Span event marking handler failure."""

    dims = base_dimensions(effect, span_id) | {"phase": [Phase.ERROR.value]}
    return span(dims, at=tspan.start, until=tspan.end, value=Message(error))


def tagged_point(tag: str, extra: Dims) -> Event:
    """Point event carrying the shared id/tag/pid dims plus extra dims."""

    dims: Dims = {
        "id": [str(uuid.uuid4())],
        "tag": [tag],
        "pid": [str(os.getpid())],
        **extra,
    }
    return point(dims, at=time.time())


def execute_start_event(fn_name: str, job_id: str) -> Event:
    """Point event marking when a worker picks up a job for execution."""

    return tagged_point(
        JobTag.EXECUTE, {"fn": [fn_name], "job_id": [job_id], "phase": [Phase.START]}
    )


def job_progress_event(completed: int, total: int | None = None) -> Event:
    """Point event emitted by job code to report intra-job progress.

    Yield via: yield EEmit(job_progress_event(completed=idx, total=len(items)))
    total may be omitted when the full count is not known upfront.
    """

    extra: Dims = {"completed": [str(completed)]}
    if total is not None:
        extra["total"] = [str(total)]
    return tagged_point(JobTag.JOB_PROGRESS, extra)


def park_event(tag: ParkTag, kind: str, caller_pid_hex: str) -> Event:
    """Point event marking an overseer parking transition (parked or woken).

    kind is "worker" (awaiting work) or "completion" (awaiting workflow end).
    """

    return tagged_point(tag, {"kind": [kind], "caller": [caller_pid_hex]})


def retry_event(fn_name: str, attempt: int, error: Exception) -> Event:
    """Point event marking a failed job attempt that the retried combinator will re-dispatch."""

    return tagged_point(
        JobTag.RETRY, {"fn": [fn_name], "attempt": [str(attempt)], "error": [type(error).__name__]}
    )


def dependency_waiting_event(label: str) -> Event:
    """Point event emitted on each dependency poll that returns unsatisfied."""

    return tagged_point(DependencyTag.WAITING, {"dep": [label]})


def dependency_satisfied_event(label: str) -> Event:
    """Point event emitted when a dependency is finally met or abandoned."""

    return tagged_point(DependencyTag.SATISFIED, {"dep": [label]})


def job_lifecycle_span(effect, job_id: str, executed_at: float, completed_at: float) -> Event:
    """Span event covering the job from when a worker picked it up to completion."""

    dims = base_dimensions(effect, job_id) | {"tag": [JobTag.JOB_LIFECYCLE.value]}
    return span(dims, at=executed_at, until=completed_at)
