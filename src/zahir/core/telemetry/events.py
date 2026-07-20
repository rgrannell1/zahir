# Helpers that translate zahir effects into bookman Event objects
import os
import time
import uuid
from dataclasses import dataclass

from bookman.bookman_types import Message
from bookman.events import Dims, Event, point, span

from zahir.core.commons.constants import JobTag, ParkTag, Phase


@dataclass
class TimeSpan:
    """A closed time interval [start, end] in seconds since epoch."""

    start: float
    end: float


def format_job_id(reply_to: bytes, sequence_number: int) -> str:
    """Form the globally unique job identifier used across telemetry."""

    return f"{reply_to.hex()}:{sequence_number}"


def get_fn_name(effect) -> str | None:
    """Get the function name from the effect, looking inside a carried JobSpec."""

    if hasattr(effect, "jobs") and effect.scalar and len(effect.jobs) == 1:
        return effect.jobs[0].fn_name

    target = getattr(effect, "job", effect)
    if hasattr(target, "fn_name"):
        return target.fn_name

    return None


def get_job_id(effect) -> str | None:
    """Form a globally unique job identifier from the sequence_number and reply_to.

    Enqueue effects carry these on their JobSpec; completion effects carry them directly.
    """

    target = getattr(effect, "job", effect)
    seq = getattr(target, "sequence_number", None)
    reply_to = getattr(target, "reply_to", None)

    if seq is not None and isinstance(reply_to, bytes):
        return format_job_id(reply_to, seq)
    if seq is None and reply_to is None and hasattr(target, "fn_name"):
        return "root"

    return None


def base_dimensions(effect, span_id: str) -> Dims:
    """We can filter all events by these dimensions."""

    pid = str(os.getpid())
    dims: Dims = {
        "id": [span_id],
        "tag": [effect.tag],
        "pid": [pid],
    }

    fn = get_fn_name(effect)
    if fn:
        dims["fn"] = [fn]

    job_id = get_job_id(effect)
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


def execute_start_event(fn_name: str, job_id: str) -> Event:
    """Point event marking when a worker picks up a job for execution."""

    dims: Dims = {
        "id": [str(uuid.uuid4())],
        "tag": [JobTag.EXECUTE],
        "pid": [str(os.getpid())],
        "fn": [fn_name],
        "job_id": [job_id],
        "phase": [Phase.START],
    }
    return point(dims, at=time.time())


def job_progress_event(completed: int, total: int | None = None) -> Event:
    """Point event emitted by job code to report intra-job progress.

    Yield via: yield EEmit(job_progress_event(completed=idx, total=len(items)))
    total may be omitted when the full count is not known upfront.
    """

    dims: Dims = {
        "id": [str(uuid.uuid4())],
        "tag": [JobTag.JOB_PROGRESS],
        "pid": [str(os.getpid())],
        "completed": [str(completed)],
    }
    if total is not None:
        dims["total"] = [str(total)]
    return point(dims, at=time.time())


def park_event(tag: ParkTag, kind: str, caller_pid_hex: str) -> Event:
    """Point event marking an overseer parking transition (parked or woken).

    kind is "worker" (awaiting work) or "completion" (awaiting workflow end).
    """

    dims: Dims = {
        "id": [str(uuid.uuid4())],
        "tag": [tag],
        "pid": [str(os.getpid())],
        "kind": [kind],
        "caller": [caller_pid_hex],
    }
    return point(dims, at=time.time())


def retry_event(fn_name: str, attempt: int, error: Exception) -> Event:
    """Point event marking a failed job attempt that the retried combinator will re-dispatch."""

    dims: Dims = {
        "id": [str(uuid.uuid4())],
        "tag": [JobTag.RETRY],
        "pid": [str(os.getpid())],
        "fn": [fn_name],
        "attempt": [str(attempt)],
        "error": [type(error).__name__],
    }
    return point(dims, at=time.time())


def job_lifecycle_span(effect, job_id: str, executed_at: float, completed_at: float) -> Event:
    """Span event covering the job from when a worker picked it up to completion."""

    dims = base_dimensions(effect, job_id) | {"tag": [JobTag.JOB_LIFECYCLE.value]}
    return span(dims, at=executed_at, until=completed_at)
