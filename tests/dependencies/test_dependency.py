from datetime import timedelta

import pytest
import time_machine

from tertius import ESleep

from zahir.core.dependencies.dependency import ImpossibleError, dependency
from zahir.core.effects import EImpossible, ESatisfied
from tests.shared import NOW


def _drive(gen, responses=None):
    """Drive a generator, sending None for all yields, and return the StopIteration value."""
    responses = responses or {}
    try:
        effect = next(gen)
        while True:
            effect = gen.send(responses.get(type(effect)))
    except StopIteration as exc:
        return exc.value


# condition returns True immediately


def test_immediate_true_yields_esatisfied():
    """Proves a condition returning True on the first call yields ESatisfied."""

    def _cond():
        return True
        yield

    effect = next(dependency(_cond))
    assert isinstance(effect, ESatisfied)


def test_immediate_true_returns_esatisfied():
    """Proves a condition returning True on the first call returns ESatisfied."""

    def _cond():
        return True
        yield

    result = _drive(dependency(_cond))
    assert isinstance(result, ESatisfied)


def test_satisfied_carries_metadata():
    """Proves metadata from a (True, metadata) return is attached to ESatisfied."""

    def _cond():
        return (True, {"key": "value"})
        yield

    effect = next(dependency(_cond))
    assert isinstance(effect, ESatisfied)
    assert effect.metadata == {"key": "value"}


def test_false_then_true_polls_and_satisfies():
    """Proves a condition returning False then True loops through ESleep before satisfying."""

    calls = iter([False, True])

    def _cond():
        return next(calls)
        yield

    gen = dependency(_cond)
    assert isinstance(next(gen), ESleep)   # False → sleep
    assert isinstance(next(gen), ESatisfied)  # True → satisfied


# ImpossibleError


def test_impossible_error_yields_eimpossible():
    """Proves ImpossibleError from the condition yields EImpossible."""

    def _cond():
        raise ImpossibleError("never")
        yield

    effect = next(dependency(_cond))
    assert isinstance(effect, EImpossible)


def test_impossible_error_reason_is_preserved():
    """Proves the ImpossibleError message becomes the EImpossible reason."""

    def _cond():
        raise ImpossibleError("specific reason")
        yield

    effect = next(dependency(_cond))
    assert effect.reason == "specific reason"


def test_impossible_error_returns_eimpossible():
    """Proves ImpossibleError from the condition returns EImpossible."""

    def _cond():
        raise ImpossibleError("never")
        yield

    result = _drive(dependency(_cond))
    assert isinstance(result, EImpossible)


# timeout


@time_machine.travel(NOW, tick=False)
def test_timeout_yields_eimpossible():
    """Proves exceeding timeout_ms yields EImpossible."""

    def _cond():
        return False
        yield

    gen = dependency(_cond, timeout_ms=1000)
    next(gen)  # False → ESleep

    with time_machine.travel(NOW + timedelta(seconds=2), tick=False):
        effect = next(gen)

    assert isinstance(effect, EImpossible)
    assert "timed out" in effect.reason
    assert "1000" in effect.reason


@time_machine.travel(NOW, tick=False)
def test_timeout_label_appears_in_reason():
    """Proves the label parameter appears in the timeout EImpossible reason."""

    def _cond():
        return False
        yield

    gen = dependency(_cond, timeout_ms=500, label="my-condition")
    next(gen)

    with time_machine.travel(NOW + timedelta(seconds=2), tick=False):
        effect = next(gen)

    assert "my-condition" in effect.reason


@time_machine.travel(NOW, tick=False)
def test_no_timeout_never_expires():
    """Proves a dependency with no timeout_ms never times out on its own."""

    def _cond():
        return False
        yield

    gen = dependency(_cond)
    for _ in range(10):
        assert isinstance(next(gen), ESleep)  # ESleep
        next(gen)                              # re-enter condition


# poll_ms


def test_custom_poll_ms_used_in_sleep():
    """Proves poll_ms controls the ESleep duration between retries."""

    def _cond():
        return False
        yield

    gen = dependency(_cond, poll_ms=999)
    effect = next(gen)
    assert isinstance(effect, ESleep)
    assert effect.ms == 999


# condition that yields effects


def test_condition_effects_pass_through():
    """Proves effects yielded by the condition are forwarded to the caller."""

    def _cond():
        yield ESleep(ms=42)
        return True

    gen = dependency(_cond)
    assert isinstance(next(gen), ESleep)   # from condition
    assert isinstance(next(gen), ESatisfied)  # from dependency()


# yield/return parity


def test_yielded_and_returned_event_are_same_object():
    """Proves the ESatisfied yielded and the StopIteration value are the same object."""

    def _cond():
        return True
        yield

    gen = dependency(_cond)
    yielded = next(gen)
    with pytest.raises(StopIteration) as exc:
        next(gen)
    assert exc.value.value is yielded
