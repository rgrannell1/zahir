# Handler wrapper that emits bookman telemetry events around effect handler calls
import os
import time
import uuid

from bookman.events import point
from tertius import EEmit

from zahir.core.combinators import wrap
from zahir.core.constants import JobTag, WorkItemTag
from zahir.core.effects import EGetJob
from zahir.core.emit import (
    end_effect_error_telemetry,
    end_effect_success_telemetry,
    start_effect_telemetry,
)


def _job_execute_event(fn_name: str) -> object:
    return point({"tag": [JobTag.EXECUTE], "pid": [str(os.getpid())], "fn": [fn_name]}, at=time.time())


def _telemetry_fn(effect):
    span_id = str(uuid.uuid4())
    start = time.time()

    yield EEmit(start_effect_telemetry(effect, span_id, start))
    try:
        result = yield
        yield EEmit(end_effect_success_telemetry(effect, span_id, start, time.time()))
        if isinstance(effect, EGetJob) and result and result[0] == WorkItemTag.JOB:
            yield EEmit(_job_execute_event(result[1]))
    except Exception as exc:
        yield EEmit(end_effect_error_telemetry(effect, span_id, start, time.time(), str(exc)))


def make_telemetry():
    """Build a handler wrapper that emits bookman Events via EEmit."""

    return wrap(_telemetry_fn)


