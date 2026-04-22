import os
import time
import uuid

from bookman.create import point, span
from bookman.events import Event
from bookman.primitives import Dims, Message
from tertius import EEmit

from zahir.core.telemetry import wrap


def _fn_name(effect) -> str | None:
    if hasattr(effect, "jobs") and effect.scalar:
        return effect.jobs[0].fn_name
    if hasattr(effect, "fn_name"):
        return effect.fn_name
    return None


def _base_dims(effect, span_id: str) -> Dims:
    dims: Dims = {
        "id": [span_id],
        "tag": [effect.tag],
        "pid": [str(os.getpid())],
    }
    fn = _fn_name(effect)
    if fn:
        dims["fn"] = [fn]
    return dims


def _extra_dims(dispatch, effect, result=None) -> Dims:
    if not dispatch:
        return {}
    handler = dispatch.get(type(effect))
    if not handler:
        return {}
    return handler(effect, result) if result is not None else handler(effect)


def _start_event(effect, span_id: str, at: float, before: dict) -> Event:
    dims = _base_dims(effect, span_id) | _extra_dims(before, effect)
    return point(dims, at=at)


def _end_success(effect, span_id: str, start: float, end: float, result, after: dict) -> Event:
    dims = _base_dims(effect, span_id) | _extra_dims(after, effect, result)
    value = Message(str(result)) if result is not None and effect.tag != "enqueue" else None
    return span(dims, at=start, until=end, value=value)


def _end_error(effect, span_id: str, start: float, end: float, error: str) -> Event:
    return span(_base_dims(effect, span_id), at=start, until=end, value=Message(error))


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
