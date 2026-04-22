# Wrapper around bookman telemetry, composes over all handler calls. Want telemetry?
# inspect events, grab properties from them, emit

import time
import uuid

from tertius import EEmit

from zahir.core.telemetry import wrap
from zahir.emit import (
    start_effect_telemetry,
    end_effect_success_telemetry,
    end_effect_error_telemetry,
)


def make_telemetry():
    """Build a handler_wrapper that emits bookman Events via EEmit."""

    def fn(effect):
        span_id = str(uuid.uuid4())
        start = time.time()

        yield EEmit(start_effect_telemetry(effect, span_id, start))
        try:
            result = yield
            yield EEmit(
                end_effect_success_telemetry(effect, span_id, start, time.time())
            )
        except Exception as exc:
            yield EEmit(
                end_effect_error_telemetry(
                    effect, span_id, start, time.time(), str(exc)
                )
            )

    return wrap(fn)
