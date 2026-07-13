# Handler wrapper that emits bookman telemetry events around effect handler calls
import time
import traceback
import uuid
from dataclasses import dataclass

from bookman.bookman_types import Message
from tertius import EEmit

from zahir.core.combinators import wrap
from zahir.core.constants import JobTag
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


# Process-local store of job execute times keyed by job_id.
# Populated by record_execute_start (called from the worker loop) rather than via handler wrappers,
# because EGetJob is excluded from wrapping (high-volume poll events with no telemetry signal).
_execute_times: dict[str, float] = {}

_JOB_END_TAGS = {JobTag.JOB_COMPLETE, JobTag.JOB_FAIL}


def record_execute_start(reply_to: bytes | None, sequence_number: int | None) -> None:
    """Record when a worker begins executing a job. Called from the worker loop on job pickup."""

    if isinstance(reply_to, bytes) and sequence_number is not None:
        _execute_times[f"{reply_to.hex()}:{sequence_number}"] = time.time()


def record_execute_start_id(job_id: str) -> None:
    """Record when a worker begins executing a job with an already-derived id."""

    _execute_times[job_id] = time.time()


def _resolve_lifecycle(effect, job_id: str | None, end: float) -> object | None:
    """Return a lifecycle span event if this is a job-end effect with a known execute time."""

    if effect.tag not in _JOB_END_TAGS or not job_id:
        return None
    executed_at = _execute_times.pop(job_id, None)
    if executed_at is None:
        return None
    return job_lifecycle_span(effect, job_id, executed_at, end)


def _setup_phase(effect, ctx: SpanContext, start: float):
    """Emit the handler-start event."""

    yield EEmit(start_effect_telemetry(effect, ctx.span_id, start))


def _format_effect_error(err: Exception) -> str:
    """Format an effect's error field as a full traceback, unwrapping JobError.cause if present."""

    cause = getattr(err, "cause", err)
    return "".join(traceback.format_exception(cause))


def _success_teardown(effect, ctx: SpanContext, tspan: TimeSpan, result):
    """Emit handler-end event and any lifecycle span that applies."""

    err = getattr(effect, "error", None)
    value = Message(_format_effect_error(err)) if err is not None else None
    yield EEmit(end_effect_success_telemetry(effect, ctx.span_id, tspan, value=value))
    lifecycle = _resolve_lifecycle(effect, ctx.job_id, tspan.end)
    if lifecycle:
        yield EEmit(lifecycle)


def _error_teardown(effect, ctx: SpanContext, start: float, exc: Exception):
    """Emit a handler-error span event."""

    tspan = TimeSpan(start, time.time())
    yield EEmit(end_effect_error_telemetry(effect, ctx.span_id, tspan, str(exc)))


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
