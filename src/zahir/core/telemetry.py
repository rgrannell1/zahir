# Handler wrapper that emits bookman telemetry events around effect handler calls
import os
import time
import traceback
import uuid
from dataclasses import dataclass

from bookman.bookman_types import Message
from bookman.events import point
from tertius import EEmit

from zahir.core.combinators import wrap
from zahir.core.constants import JobTag, WorkItemTag
from zahir.core.effects import EGetJob
from zahir.core.emit import (
    TimeSpan,
    end_effect_error_telemetry,
    end_effect_success_telemetry,
    get_job_id,
    job_lifecycle_span,
    start_effect_telemetry,
)


@dataclass
class SpanContext:
    """Identifiers for one telemetry span."""

    span_id: str
    job_id: str | None

# Process-local store of enqueue times keyed by job_id.
# Safe to use here because ENQUEUE and JOB_COMPLETE are always handled in the same process.
_enqueue_times: dict[str, float] = {}

_JOB_END_TAGS = {JobTag.JOB_COMPLETE, JobTag.JOB_FAIL}


def _job_execute_event(fn_name: str) -> object:
    return point({"tag": [JobTag.EXECUTE], "pid": [str(os.getpid())], "fn": [fn_name]}, at=time.time())


def _record_enqueue(effect, job_id: str | None, start: float) -> None:
    """Store the enqueue start time so a lifecycle span can be emitted at completion."""

    if effect.tag == JobTag.ENQUEUE and job_id:
        _enqueue_times[job_id] = start


def _resolve_lifecycle(effect, job_id: str | None, end: float) -> object | None:
    """Return a lifecycle span event if this is a job-end effect with a known enqueue time."""

    if effect.tag not in _JOB_END_TAGS or not job_id:
        return None
    enqueued_at = _enqueue_times.pop(job_id, None)
    if enqueued_at is None:
        return None
    return job_lifecycle_span(effect, job_id, enqueued_at, end)


def _resolve_execute(effect, result) -> object | None:
    """Return a job:execute event if the EGetJob handler returned a job work item."""

    if isinstance(effect, EGetJob) and result and result[0] == WorkItemTag.JOB:
        return _job_execute_event(result[1])
    return None


def _setup_phase(effect, ctx: SpanContext, start: float):
    """Emit the handler-start event; record enqueue time if this is an ENQUEUE effect."""

    _record_enqueue(effect, ctx.job_id, start)
    yield EEmit(start_effect_telemetry(effect, ctx.span_id, start))


def _format_effect_error(err: Exception) -> str:
    """Format an effect's error field as a full traceback, unwrapping JobError.cause if present."""

    cause = getattr(err, "cause", err)
    return "".join(traceback.format_exception(cause))


def _success_teardown(effect, ctx: SpanContext, tspan: TimeSpan, result):
    """Emit handler-end event and any lifecycle or execute events that apply."""

    err = getattr(effect, "error", None)
    value = Message(_format_effect_error(err)) if err is not None else None
    yield EEmit(end_effect_success_telemetry(effect, ctx.span_id, tspan, value=value))
    lifecycle = _resolve_lifecycle(effect, ctx.job_id, tspan.end)
    if lifecycle:
        yield EEmit(lifecycle)
    execute = _resolve_execute(effect, result)
    if execute:
        yield EEmit(execute)


def _error_teardown(effect, ctx: SpanContext, start: float, exc: Exception):
    """Emit a handler-error span event."""

    trace = "".join(traceback.format_exception(exc))
    yield EEmit(end_effect_error_telemetry(effect, ctx.span_id, TimeSpan(start, time.time()), trace))


def _telemetry_fn(effect):
    """Emit telemetry events around the handler call."""

    ctx = SpanContext(span_id=str(uuid.uuid4()), job_id=get_job_id(effect))
    start = time.time()
    yield from _setup_phase(effect, ctx, start)

    try:
        result = yield
        yield from _success_teardown(effect, ctx, TimeSpan(start, time.time()), result)
    except Exception as exc:  # noqa: BLE001
        yield from _error_teardown(effect, ctx, start, exc)


def make_telemetry():
    """Build a handler wrapper that emits bookman Events via EEmit."""

    return wrap(_telemetry_fn)
