# Helpers that translate zahir effects into bookman Event objects
import os

from bookman.create import point, span
from bookman.events import Event
from bookman.primitives import Dims, Message

# TODO this all needs work

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
