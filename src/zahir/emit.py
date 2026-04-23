# Helpers that translate zahir effects into bookman Event objects
import os

from bookman.create import point, span
from bookman.events import Event
from bookman.primitives import Dims

# Event lifecycle phases — emitted as the 'phase' dim on every bookman event
PHASE_START = "start"
PHASE_END = "end"
PHASE_ERROR = "error"


def get_fn_name(effect) -> str | None:
    if hasattr(effect, "jobs") and effect.scalar and len(effect.jobs) == 1:
        return effect.jobs[0].fn_name

    if hasattr(effect, "fn_name"):
        return effect.fn_name

    return None


def base_dimensions(effect, span_id: str) -> Dims:
    pid = str(os.getpid())
    dims: Dims = {
        "id": [span_id],
        "tag": [effect.tag],
        "pid": [pid],
    }

    fn = get_fn_name(effect)

    if fn:
        dims["fn"] = [fn]

    return dims


def start_effect_telemetry(effect, span_id: str, at: float) -> Event:
    """Point event marking when a handler began."""
    dims = base_dimensions(effect, span_id) | {"phase": [PHASE_START]}
    return point(dims, at=at)


def end_effect_success_telemetry(effect, span_id: str, start: float, end: float) -> Event:
    """Span event marking successful handler completion."""
    dims = base_dimensions(effect, span_id) | {"phase": [PHASE_END]}
    return span(dims, at=start, until=end)


def end_effect_error_telemetry(effect, span_id: str, start: float, end: float, error: str) -> Event:
    """Span event marking handler failure."""
    dims = base_dimensions(effect, span_id) | {"phase": [PHASE_ERROR]}
    return span(dims, at=start, until=end, value=error)
