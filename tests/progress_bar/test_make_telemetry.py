import pytest
from tertius import EEmit

from bookman.events import Event
from zahir.core.effects import EAwait
from zahir.core.zahir_types import JobSpec
from zahir.core.telemetry import make_telemetry


def _drive(gen):
    """Collect all yielded effects and return value from a generator."""
    effects = []
    try:
        value = next(gen)
        while True:
            effects.append(value)
            value = gen.send(None)
    except StopIteration as exc:
        return effects, exc.value


def _make_handler(return_value):
    def handler(effect):
        return return_value
        yield

    return handler


def _make_raising_handler(exc):
    def handler(effect):
        raise exc
        yield

    return handler


def _emitted(effects):
    """Extract bookman Events from EEmit effects."""
    return [
        e.body for e in effects if isinstance(e, EEmit) and isinstance(e.body, Event)
    ]


def test_emits_start_event_before_handler():
    "Proves the first emitted event is a point event"
    wrapper = make_telemetry()
    handler = wrapper(_make_handler("result"))
    effects, _ = _drive(handler(EAwait(jobs=[JobSpec("job_a")])))
    events = _emitted(effects)
    assert events[0].kind == "point"


def test_emits_span_end_after_handler():
    "Proves the last emitted event is a span event"
    wrapper = make_telemetry()
    handler = wrapper(_make_handler("result"))
    effects, _ = _drive(handler(EAwait(jobs=[JobSpec("job_a")])))
    events = _emitted(effects)
    assert events[-1].kind == "span"


def test_emits_exactly_two_events_on_success():
    "Proves exactly one start and one end event are emitted per handler invocation"
    wrapper = make_telemetry()
    handler = wrapper(_make_handler("result"))
    effects, _ = _drive(handler(EAwait(jobs=[JobSpec("job_a")])))
    assert len(_emitted(effects)) == 2


def test_start_and_end_share_span_id():
    "Proves start and end events carry the same id dimension for correlation"
    wrapper = make_telemetry()
    handler = wrapper(_make_handler("result"))
    effects, _ = _drive(handler(EAwait(jobs=[JobSpec("job_a")])))
    events = _emitted(effects)
    assert events[0].dim("id") == events[1].dim("id")


def test_start_event_carries_fn_name():
    "Proves the fn dimension on the start event reflects the job function name"
    wrapper = make_telemetry()
    handler = wrapper(_make_handler("result"))
    effects, _ = _drive(handler(EAwait(jobs=[JobSpec("my_job")])))
    start = next(e for e in _emitted(effects) if e.kind == "point")
    assert start.dim("fn") == "my_job"


def test_end_event_has_non_negative_duration():
    "Proves the span end event records a non-negative duration"
    wrapper = make_telemetry()
    handler = wrapper(_make_handler("result"))
    effects, _ = _drive(handler(EAwait(jobs=[JobSpec("job_a")])))
    end = next(e for e in _emitted(effects) if e.kind == "span")
    assert end.duration("ms") >= 0.0


def test_emits_span_end_with_error_message_on_exception():
    "Proves a handler exception is captured as a Message value on the end span"
    wrapper = make_telemetry()
    handler = wrapper(_make_raising_handler(ValueError("boom")))
    gen = handler(EAwait(jobs=[JobSpec("job_a")]))
    effects = []
    with pytest.raises(ValueError):
        value = next(gen)
        while True:
            effects.append(value)
            value = gen.send(None)
    end_events = [e for e in _emitted(effects) if e.kind == "span"]
    assert len(end_events) == 1
    assert end_events[0].value == "boom"


def test_handler_return_value_preserved_through_telemetry():
    "Proves the telemetry wrapper does not swallow or alter the handler return value"
    wrapper = make_telemetry()
    handler = wrapper(_make_handler(42))
    _, value = _drive(handler(EAwait(jobs=[JobSpec("job_a")])))
    assert value == 42
