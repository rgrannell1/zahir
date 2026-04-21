import os
import time
import uuid

from tertius import EEmit

from zahir.core.telemetry import wrap
from zahir.progress_bar.events import ZahirSpanEnd, ZahirTelemetryEvent


def _default_attrs(effect):
    """Extract well-known fields from any effect that carries them."""
    attrs = {"pid": os.getpid()}

    if hasattr(effect, "jobs"):
        if effect.scalar:
            attrs["fn_name"] = effect.jobs[0].fn_name
        else:
            attrs["fn_names"] = [spec.fn_name for spec in effect.jobs]
            attrs["count"] = len(effect.jobs)
    elif hasattr(effect, "fn_name"):
        attrs["fn_name"] = effect.fn_name

    return attrs


def _before_attrs(dispatch, effect):
    """Get attributes for the start event, merging defaults with any user-provided handler."""
    attrs = _default_attrs(effect)
    if dispatch:
        handler = dispatch.get(type(effect))
        if handler:
            attrs |= handler(effect)
    return attrs


def _after_attrs(dispatch, effect, result):
    """Get attributes for the end event, merging defaults with any user-provided handler."""
    attrs = _default_attrs(effect)
    if dispatch:
        handler = dispatch.get(type(effect))
        if handler:
            attrs |= handler(effect, result)
    return attrs


def _span_start(span_id, effect, attrs, timestamp):
    return ZahirTelemetryEvent(
        span_id=span_id,
        tag=effect.tag,
        event="start",
        timestamp=timestamp,
        attributes=attrs,
    )


def _span_end(span_id, effect, attrs, start, end, error=None):
    return ZahirSpanEnd(
        span_id=span_id,
        tag=effect.tag,
        event="end",
        timestamp=end,
        attributes=attrs,
        duration_ms=(end - start) * 1000,
        error=error,
    )


def make_telemetry(before=None, after=None):
    """Build a handler_wrapper that emits ZahirTelemetryEvent and ZahirSpanEnd via EEmit.

    before: dict mapping effect class to Callable[[effect], dict]
    after:  dict mapping effect class to Callable[[effect, result], dict]

    span_id and timestamps are managed automatically. Exceptions from the handler
    are captured and recorded in ZahirSpanEnd.error before being re-raised.
    """

    def fn(effect):
        span_id = str(uuid.uuid4())
        start = time.time()

        yield EEmit(_span_start(span_id, effect, _before_attrs(before, effect), start))
        try:
            result = yield  # seam — handler runs here
            yield EEmit(
                _span_end(
                    span_id,
                    effect,
                    _after_attrs(after, effect, result),
                    start,
                    time.time(),
                )
            )
        except Exception as exc:
            yield EEmit(
                _span_end(span_id, effect, {}, start, time.time(), error=str(exc))
            )

    return wrap(fn)
