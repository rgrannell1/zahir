from datetime import timedelta

import pytest
import time_machine

from tertius import EEmit, ESleep

from zahir.core.dependencies.dependency import ImpossibleError, check, dependency
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


def test_immediate_true_emits_satisfied():
    """Proves a condition returning True on the first call emits satisfied."""

    def _cond():
        return True
        yield

    emit = next(dependency(_cond))
    assert isinstance(emit, EEmit)
    assert emit.body[0] == "satisfied"


def test_immediate_true_returns_satisfied_tuple():
    """Proves a condition returning True on the first call returns a satisfied tuple."""

    def _cond():
        return True
        yield

    result = _drive(dependency(_cond))
    assert result[0] == "satisfied"


def test_satisfied_carries_metadata():
    """Proves metadata from a (True, metadata) return is attached to the satisfied body."""

    def _cond():
        return (True, {"key": "value"})
        yield

    emit = next(dependency(_cond))
    assert emit.body[0] == "satisfied"
    assert emit.body[1] == {"key": "value"}


def test_false_then_true_polls_and_satisfies():
    """Proves a condition returning False then True loops through ESleep before satisfying."""

    calls = iter([False, True])

    def _cond():
        return next(calls)
        yield

    gen = dependency(_cond)
    assert isinstance(next(gen), ESleep)  # False → sleep
    emit = next(gen)
    assert emit.body[0] == "satisfied"  # True → satisfied


# ImpossibleError


def test_impossible_error_emits_impossible():
    """Proves ImpossibleError from the condition emits impossible."""

    def _cond():
        raise ImpossibleError("never")
        yield

    emit = next(dependency(_cond))
    assert isinstance(emit, EEmit)
    assert emit.body[0] == "impossible"


def test_impossible_error_reason_is_preserved():
    """Proves the ImpossibleError message becomes the impossible reason."""

    def _cond():
        raise ImpossibleError("specific reason")
        yield

    emit = next(dependency(_cond))
    assert emit.body[1] == "specific reason"


def test_impossible_error_returns_impossible_tuple():
    """Proves ImpossibleError from the condition returns an impossible tuple."""

    def _cond():
        raise ImpossibleError("never")
        yield

    result = _drive(dependency(_cond))
    assert result[0] == "impossible"


# timeout


@time_machine.travel(NOW, tick=False)
def test_timeout_emits_impossible():
    """Proves exceeding timeout_ms emits impossible."""

    def _cond():
        return False
        yield

    gen = dependency(_cond, timeout_ms=1000)
    next(gen)  # False → ESleep

    with time_machine.travel(NOW + timedelta(seconds=2), tick=False):
        emit = next(gen)

    assert emit.body[0] == "impossible"
    assert "timed out" in emit.body[1]
    assert "1000" in emit.body[1]


@time_machine.travel(NOW, tick=False)
def test_timeout_label_appears_in_reason():
    """Proves the label parameter appears in the timeout impossible reason."""

    def _cond():
        return False
        yield

    gen = dependency(_cond, timeout_ms=500, label="my-condition")
    next(gen)

    with time_machine.travel(NOW + timedelta(seconds=2), tick=False):
        emit = next(gen)

    assert "my-condition" in emit.body[1]


@time_machine.travel(NOW, tick=False)
def test_no_timeout_never_expires():
    """Proves a dependency with no timeout_ms never times out on its own."""

    def _cond():
        return False
        yield

    gen = dependency(_cond)
    for _ in range(10):
        assert isinstance(next(gen), ESleep)
        next(gen)


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
    assert isinstance(next(gen), ESleep)  # from condition
    emit = next(gen)
    assert emit.body[0] == "satisfied"  # from dependency()


# emit/return parity


def test_emitted_and_returned_body_are_same_object():
    """Proves the EEmit body and the StopIteration value are the same object."""

    def _cond():
        return True
        yield

    gen = dependency(_cond)
    emit = next(gen)
    with pytest.raises(StopIteration) as exc:
        next(gen)
    assert exc.value.value is emit.body


# check — single-shot evaluation


def test_check_true_emits_satisfied():
    """Proves check() emits satisfied when the condition returns True."""

    def _cond():
        return True
        yield

    emit = next(check(_cond))
    assert isinstance(emit, EEmit)
    assert emit.body[0] == "satisfied"


def test_check_true_returns_satisfied_tuple():
    """Proves check() returns a satisfied tuple when the condition is met."""

    def _cond():
        return True
        yield

    result = _drive(check(_cond))
    assert result[0] == "satisfied"


def test_check_false_emits_impossible():
    """Proves check() emits impossible when the condition returns False."""

    def _cond():
        return False
        yield

    emit = next(check(_cond))
    assert isinstance(emit, EEmit)
    assert emit.body[0] == "impossible"


def test_check_false_returns_impossible_tuple():
    """Proves check() returns an impossible tuple when the condition is not met."""

    def _cond():
        return False
        yield

    result = _drive(check(_cond))
    assert result[0] == "impossible"


def test_check_false_reason_contains_label():
    """Proves the label appears in the impossible reason when the condition is not met."""

    def _cond():
        return False
        yield

    emit = next(check(_cond, label="my-gate"))
    assert "my-gate" in emit.body[1]


def test_check_impossible_error_emits_impossible():
    """Proves check() emits impossible when the condition raises ImpossibleError."""

    def _cond():
        raise ImpossibleError("hard stop")
        yield

    emit = next(check(_cond))
    assert emit.body[0] == "impossible"


def test_check_impossible_error_reason_preserved():
    """Proves the ImpossibleError message is preserved in the impossible reason."""

    def _cond():
        raise ImpossibleError("hard stop")
        yield

    emit = next(check(_cond))
    assert emit.body[1] == "hard stop"


def test_check_does_not_retry_on_false():
    """Proves check() never yields ESleep — it stops after one evaluation."""

    calls = 0

    def _cond():
        nonlocal calls
        calls += 1
        return False
        yield

    _drive(check(_cond))
    assert calls == 1


def test_check_metadata_attached_on_satisfied():
    """Proves metadata from a (True, metadata) return is attached to the satisfied body."""

    def _cond():
        return (True, {"key": "val"})
        yield

    emit = next(check(_cond))
    assert emit.body[1] == {"key": "val"}


def test_check_condition_effects_pass_through():
    """Proves effects yielded by the condition are forwarded to the caller."""

    def _cond():
        yield ESleep(ms=7)
        return True

    gen = check(_cond)
    assert isinstance(next(gen), ESleep)
    emit = next(gen)
    assert emit.body[0] == "satisfied"
