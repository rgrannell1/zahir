import pytest
from tertius import EEmit

from zahir.core.effects import EAwait
from zahir.core.zahir_types import JobSpec
from zahir.core.telemetry import wrap


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
    """Handler stub that returns a fixed value without yielding."""

    def handler(effect):
        return return_value
        yield

    return handler


# setup and teardown phases


def test_wrap_runs_setup_before_handler():
    """Proves setup in f runs before the handler is called."""

    order = []

    def f(effect):
        order.append("setup")
        yield
        order.append("teardown")

    handler = _make_handler("result")
    wrapped = wrap(f)(handler)

    list(_drive(wrapped(EAwait(jobs=[JobSpec("job")]))))
    assert order == ["setup", "teardown"]


def test_wrap_passes_handler_return_value_to_f():
    """Proves f receives the handler's return value via send."""

    received = []

    def f(effect):
        result = yield
        received.append(result)

    wrapped = wrap(f)(_make_handler("my_result"))
    _drive(wrapped(EAwait(jobs=[JobSpec("job")])))

    assert received == ["my_result"]


def test_wrap_preserves_handler_return_value():
    """Proves wrap does not alter the return value of the handler."""

    def f(effect):
        yield

    wrapped = wrap(f)(_make_handler(42))
    _, value = _drive(wrapped(EAwait(jobs=[JobSpec("job")])))

    assert value == 42


def test_wrap_f_receives_effect():
    """Proves f is called with the effect the job yielded."""

    received = []

    def f(effect):
        received.append(effect)
        yield

    effect = EAwait(jobs=[JobSpec("job")])
    wrapped = wrap(f)(_make_handler(None))
    _drive(wrapped(effect))

    assert received == [effect]


# teardown yields


def test_wrap_propagates_teardown_yields():
    """Proves effects yielded by f during teardown are propagated to the caller."""

    def f(effect):
        yield
        yield EEmit("done")

    wrapped = wrap(f)(_make_handler(None))
    effects, _ = _drive(wrapped(EAwait(jobs=[JobSpec("job")])))

    assert EEmit("done") in effects


def test_wrap_propagates_multiple_teardown_yields():
    """Proves multiple effects yielded during teardown are all propagated."""

    def f(effect):
        yield
        yield EEmit("first")
        yield EEmit("second")

    wrapped = wrap(f)(_make_handler(None))
    effects, _ = _drive(wrapped(EAwait(jobs=[JobSpec("job")])))

    assert effects == [EEmit("first"), EEmit("second")]


def test_wrap_teardown_with_no_yields_does_not_error():
    """Proves a teardown phase that yields nothing completes without error."""

    def f(effect):
        yield
        _ = 1 + 1  # teardown with no yields

    wrapped = wrap(f)(_make_handler("ok"))
    _, value = _drive(wrapped(EAwait(jobs=[JobSpec("job")])))

    assert value == "ok"


# stacking via reduce


def test_wrap_stacks_correctly_via_reduce():
    """Proves two wrapped wrappers both fire in order around the handler."""

    from functools import reduce

    order = []

    def first(effect):
        order.append("first_setup")
        yield
        order.append("first_teardown")

    def second(effect):
        order.append("second_setup")
        yield
        order.append("second_teardown")

    handler = _make_handler("result")
    wrapped = reduce(lambda h, w: w(h), [wrap(first), wrap(second)], handler)
    _drive(wrapped(EAwait(jobs=[JobSpec("job")])))

    assert order == ["second_setup", "first_setup", "first_teardown", "second_teardown"]


# exception handling


def _make_raising_handler(exc):
    """Handler stub that raises the given exception."""

    def handler(effect):
        raise exc
        yield

    return handler


def test_wrap_reraises_handler_exception():
    """Proves wrap re-raises exceptions from the handler after teardown."""

    def fn(effect):
        yield

    wrapped = wrap(fn)(_make_raising_handler(ValueError("boom")))
    with pytest.raises(ValueError, match="boom"):
        _drive(wrapped(EAwait(jobs=[JobSpec("job")])))


def test_wrap_throws_exception_into_fn_teardown():
    """Proves wrap throws the handler exception into fn's seam so teardown can observe it."""

    errors = []

    def fn(effect):
        try:
            yield  # seam — throw lands here on handler exception
        except Exception as exc:
            errors.append(str(exc))

    wrapped = wrap(fn)(_make_raising_handler(ValueError("boom")))
    with pytest.raises(ValueError):
        _drive(wrapped(EAwait(jobs=[JobSpec("job")])))

    assert errors == ["boom"]


def test_wrap_propagates_teardown_yields_on_exception():
    """Proves teardown yields from fn are propagated even when the handler raised."""

    effects_seen = []

    def fn(effect):
        try:
            yield  # seam — throw lands here
        except Exception:
            yield EEmit("error_event")

    gen = wrap(fn)(_make_raising_handler(ValueError("boom")))(EAwait(jobs=[JobSpec("job")]))
    with pytest.raises(ValueError):
        value = next(gen)
        while True:
            effects_seen.append(value)
            value = gen.send(None)

    assert EEmit("error_event") in effects_seen
