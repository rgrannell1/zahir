# Builds a handler wrapper that emits bookman telemetry events around effect handlers
import time
import uuid

from tertius import EEmit

from zahir.core.telemetry import wrap
from zahir.emit import _end_error, _end_success, _start_event


def make_telemetry(before=None, after=None):
    """Build a handler_wrapper that emits bookman Events via EEmit.

    before: dict mapping effect class to Callable[[effect], Dims]
    after:  dict mapping effect class to Callable[[effect, result], Dims]

    Extra dims from before/after are merged into the emitted event.
    """

    def fn(effect):
        span_id = str(uuid.uuid4())
        start = time.time()

        yield EEmit(_start_event(effect, span_id, start, before))
        try:
            result = yield
            yield EEmit(_end_success(effect, span_id, start, time.time(), result, after))
        except Exception as exc:
            yield EEmit(_end_error(effect, span_id, start, time.time(), str(exc)))

    return wrap(fn)
