# Helpers that translate zahir effects into bookman Event objects
import os
import time
import uuid
from dataclasses import dataclass

from bookman.bookman_types import Message
from bookman.events import Dims, Event, point, span

from zahir.core.constants import JobTag, Phase


@dataclass
class TimeSpan:
    """A closed time interval [start, end] in seconds since epoch."""

    start: float
    end: float


def get_fn_name(effect) -> str | None:
    """Get the function name from the effect."""

    if hasattr(effect, "jobs") and effect.scalar and len(effect.jobs) == 1:
        return effect.jobs[0].fn_name

    if hasattr(effect, "fn_name"):
        return effect.fn_name

    return None


def get_job_id(effect) -> str | None:
    """Form a globally unique job identifier from the sequence_number and reply_to."""

    seq = getattr(effect, "sequence_number", None)
    reply_to = getattr(effect, "reply_to", None)

    if seq is not None and isinstance(reply_to, bytes):
        return f"{reply_to.hex()}:{seq}"

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

    dims = base_dimensions(effect, span_id) | {"phase": [Phase.START]}
    return point(dims, at=at)


def end_effect_success_telemetry(
    effect, span_id: str, tspan: TimeSpan, value: Message | None = None
) -> Event:
    """Span event marking successful handler completion."""

    dims = base_dimensions(effect, span_id) | {"phase": [Phase.END]}
    return span(dims, at=tspan.start, until=tspan.end, value=value)


def end_effect_error_telemetry(effect, span_id: str, tspan: TimeSpan, error: str) -> Event:
    """Span event marking handler failure."""

    dims = base_dimensions(effect, span_id) | {"phase": [Phase.ERROR]}
    return span(dims, at=tspan.start, until=tspan.end, value=error)


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


def job_lifecycle_span(effect, job_id: str, executed_at: float, completed_at: float) -> Event:
    """Span event covering the job from when a worker picked it up to completion."""

    dims = base_dimensions(effect, job_id) | {"tag": [JobTag.JOB_LIFECYCLE]}
    return span(dims, at=executed_at, until=completed_at)
