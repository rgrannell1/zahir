from tertius import EEmit

from zahir.core.effects import EAwait
from zahir.core.zahir_types import JobSpec
from zahir.progress_bar.events import ZahirSpanEnd, ZahirTelemetryEvent
from zahir.progress_bar.telemetry import make_telemetry


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
    """Extract ZahirTelemetryEvent bodies from EEmit effects."""
    return [
        e.body
        for e in effects
        if isinstance(e, EEmit) and isinstance(e.body, ZahirTelemetryEvent)
    ]


# emitted event types


def test_emits_start_event_before_handler():
    wrapper = make_telemetry()
    handler = wrapper(_make_handler("result"))
    effects, _ = _drive(handler(EAwait(jobs=[JobSpec("job_a")])))
    events = _emitted(effects)
    assert isinstance(events[0], ZahirTelemetryEvent)
    assert not isinstance(events[0], ZahirSpanEnd)
    assert events[0].event == "start"


def test_emits_span_end_after_handler():
    wrapper = make_telemetry()
    handler = wrapper(_make_handler("result"))
    effects, _ = _drive(handler(EAwait(jobs=[JobSpec("job_a")])))
    events = _emitted(effects)
    assert isinstance(events[-1], ZahirSpanEnd)


def test_emits_exactly_two_events_on_success():
    wrapper = make_telemetry()
    handler = wrapper(_make_handler("result"))
    effects, _ = _drive(handler(EAwait(jobs=[JobSpec("job_a")])))
    events = _emitted(effects)
    assert len(events) == 2


def test_start_and_end_share_span_id():
    wrapper = make_telemetry()
    handler = wrapper(_make_handler("result"))
    effects, _ = _drive(handler(EAwait(jobs=[JobSpec("job_a")])))
    events = _emitted(effects)
    assert events[0].span_id == events[1].span_id


# fn_name in attributes


def test_start_event_carries_fn_name():
    wrapper = make_telemetry()
    handler = wrapper(_make_handler("result"))
    effects, _ = _drive(handler(EAwait(jobs=[JobSpec("my_job")])))
    start = next(e for e in _emitted(effects) if e.event == "start")
    assert start.attributes.get("fn_name") == "my_job"


def test_end_event_has_positive_duration():
    wrapper = make_telemetry()
    handler = wrapper(_make_handler("result"))
    effects, _ = _drive(handler(EAwait(jobs=[JobSpec("job_a")])))
    end = next(e for e in _emitted(effects) if isinstance(e, ZahirSpanEnd))
    assert end.duration_ms >= 0.0


# error path


def test_emits_span_end_with_error_on_exception():
    import pytest

    wrapper = make_telemetry()
    handler = wrapper(_make_raising_handler(ValueError("boom")))
    gen = handler(EAwait(jobs=[JobSpec("job_a")]))
    effects = []
    with pytest.raises(ValueError):
        value = next(gen)
        while True:
            effects.append(value)
            value = gen.send(None)
    end_events = [e for e in _emitted(effects) if isinstance(e, ZahirSpanEnd)]
    assert len(end_events) == 1
    assert end_events[0].error == "boom"


def test_handler_return_value_preserved_through_telemetry():
    wrapper = make_telemetry()
    handler = wrapper(_make_handler(42))
    _, value = _drive(handler(EAwait(jobs=[JobSpec("job_a")])))
    assert value == 42


# custom before/after dispatchers


def test_before_dispatcher_merges_attrs_into_start_event():
    extra = {"custom_key": "custom_value"}
    wrapper = make_telemetry(before={EAwait: lambda e: extra})
    handler = wrapper(_make_handler("result"))
    effects, _ = _drive(handler(EAwait(jobs=[JobSpec("job_a")])))
    start = next(e for e in _emitted(effects) if e.event == "start")
    assert start.attributes.get("custom_key") == "custom_value"


def test_after_dispatcher_merges_attrs_into_end_event():
    wrapper = make_telemetry(after={EAwait: lambda e, r: {"result_len": len(r)}})
    handler = wrapper(_make_handler("hello"))
    effects, _ = _drive(handler(EAwait(jobs=[JobSpec("job_a")])))
    end = next(e for e in _emitted(effects) if isinstance(e, ZahirSpanEnd))
    assert end.attributes.get("result_len") == 5
