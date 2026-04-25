# Handler wrapper that emits bookman telemetry events around effect handler calls
import time
import uuid

from tertius import EEmit

from zahir.core.combinators import wrap
from zahir.core.emit import (
    end_effect_error_telemetry,
    end_effect_success_telemetry,
    start_effect_telemetry,
)


def _telemetry_fn(effect):
    span_id = str(uuid.uuid4())
    start = time.time()

    yield EEmit(start_effect_telemetry(effect, span_id, start))
    try:
        result = yield
        yield EEmit(end_effect_success_telemetry(effect, span_id, start, time.time()))
    except Exception as exc:
        yield EEmit(end_effect_error_telemetry(effect, span_id, start, time.time(), str(exc)))


def make_telemetry():
    """Build a handler wrapper that emits bookman Events via EEmit."""

    return wrap(_telemetry_fn)
