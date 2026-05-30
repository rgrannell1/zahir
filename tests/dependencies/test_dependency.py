from datetime import timedelta

import time_machine
from tertius import EEmit, ESleep

from tests.shared import NOW, drain_to
from zahir.core.constants import DependencyState
from zahir.core.dependencies.dependency import check, dependency

# condition returns satisfied immediately


def test_immediate_satisfied_emits_satisfied():
    """Proves a condition returning satisfied on the first call emits satisfied."""

    def _cond():
        return (DependencyState.SATISFIED, None)
        yield

    emit = next(dependency(_cond))
    assert isinstance(emit, EEmit)
    assert emit.body[0] == "satisfied"


def test_immediate_satisfied_returns_satisfied_tuple():
    """Proves a condition returning satisfied on the first call returns a satisfied tuple."""

    def _cond():
        return (DependencyState.SATISFIED, None)
        yield

    _, result = drain_to(dependency(_cond))
    assert result[0] == "satisfied"


def test_satisfied_carries_metadata():
    """Proves metadata from a satisfied ConditionResult is attached to the satisfied body."""

    def _cond():
        return (DependencyState.SATISFIED, {"key": "value"})
        yield

    emit = next(dependency(_cond))
    assert emit.body[0] == "satisfied"
    assert emit.body[1] == {"key": "value"}


def test_unsatisfied_then_satisfied_polls_and_satisfies():
    """Proves looping through ESleep when unsatisfied before returning satisfied."""

    calls = iter([(DependencyState.UNSATISFIED, None), (DependencyState.SATISFIED, None)])

    def _cond():
        return next(calls)
        yield

    effects, _ = drain_to(dependency(_cond))
    assert any(isinstance(e, ESleep) for e in effects)
    emits = [e for e in effects if isinstance(e, EEmit)]
    assert any(e.body[0] == "satisfied" for e in emits)


# condition returns impossible


def test_impossible_condition_emits_impossible():
    """Proves a condition returning impossible terminates polling and emits impossible."""

    def _cond():
        return (DependencyState.IMPOSSIBLE, {"reason": "never"})
        yield

    emit = next(dependency(_cond))
    assert isinstance(emit, EEmit)
    assert emit.body[0] == "impossible"


def test_impossible_condition_metadata_preserved():
    """Proves metadata from an impossible ConditionResult is preserved in the impossible body."""

    def _cond():
        return (DependencyState.IMPOSSIBLE, {"reason": "specific reason"})
        yield

    emit = next(dependency(_cond))
    assert emit.body[1] == {"reason": "specific reason"}


def test_impossible_condition_returns_impossible_tuple():
    """Proves a condition returning impossible causes dependency to return an impossible tuple."""

    def _cond():
        return (DependencyState.IMPOSSIBLE, None)
        yield

    _, result = drain_to(dependency(_cond))
    assert result[0] == "impossible"


# timeout


@time_machine.travel(NOW, tick=False)
def test_timeout_emits_impossible():
    """Proves exceeding timeout_ms emits impossible."""

    def _cond():
        return (DependencyState.UNSATISFIED, None)
        yield

    gen = dependency(_cond, timeout_ms=1000)
    next(gen)  # advance through one retry: EEmit(waiting)
    next(gen)  # advance through one retry: ESleep

    with time_machine.travel(NOW + timedelta(seconds=2), tick=False):
        emits, _ = drain_to(gen, EEmit)

    assert emits[0].body[0] == "impossible"
    assert "timed out" in emits[0].body[1]["reason"]
    assert "1000" in emits[0].body[1]["reason"]


@time_machine.travel(NOW, tick=False)
def test_timeout_label_appears_in_reason():
    """Proves the label parameter appears in the timeout impossible reason."""

    def _cond():
        return (DependencyState.UNSATISFIED, None)
        yield

    gen = dependency(_cond, timeout_ms=500, label="my-condition")
    next(gen)  # advance through one retry: EEmit(waiting)
    next(gen)  # advance through one retry: ESleep

    with time_machine.travel(NOW + timedelta(seconds=2), tick=False):
        emits, _ = drain_to(gen, EEmit)

    assert "my-condition" in emits[0].body[1]["reason"]


@time_machine.travel(NOW, tick=False)
def test_zero_timeout_expires_immediately():
    """Proves timeout_ms=0 times out before the first poll rather than never expiring."""

    # condition is satisfiable: a working zero timeout must fire before it is checked
    def _cond():
        return (DependencyState.SATISFIED, None)
        yield

    gen = dependency(_cond, timeout_ms=0)
    emits, _ = drain_to(gen, EEmit)

    assert emits[0].body[0] == "impossible"
    assert "timed out" in emits[0].body[1]["reason"]
    assert "0" in emits[0].body[1]["reason"]


@time_machine.travel(NOW, tick=False)
def test_no_timeout_never_expires():
    """Proves a dependency with no timeout_ms never times out on its own."""

    def _cond():
        return (DependencyState.UNSATISFIED, None)
        yield

    gen = dependency(_cond)
    for _ in range(10):
        # drive through one retry cycle and verify no impossible is emitted
        retry = [next(gen), next(gen)]
        assert not any(isinstance(e, EEmit) and e.body[0] == "impossible" for e in retry)


# poll_ms


def test_custom_poll_ms_used_in_sleep():
    """Proves poll_ms controls the ESleep duration between retries."""

    calls = iter([(DependencyState.UNSATISFIED, None), (DependencyState.SATISFIED, None)])

    def _cond():
        return next(calls)
        yield

    effects, _ = drain_to(dependency(_cond, poll_ms=999))
    sleeps = [e for e in effects if isinstance(e, ESleep)]
    assert sleeps
    assert all(e.ms == 999 for e in sleeps)


# condition that yields effects


def test_condition_effects_pass_through():
    """Proves effects yielded by the condition are forwarded to the caller."""

    def _cond():
        yield ESleep(ms=42)
        return (DependencyState.SATISFIED, None)

    effects, _ = drain_to(dependency(_cond))
    assert any(isinstance(e, ESleep) and e.ms == 42 for e in effects)
    emits = [e for e in effects if isinstance(e, EEmit)]
    assert any(e.body[0] == "satisfied" for e in emits)


# emit/return parity


def test_emitted_and_returned_body_are_same_object():
    """Proves the EEmit body and the StopIteration value are the same object."""

    def _cond():
        return (DependencyState.SATISFIED, None)
        yield

    emits, return_value = drain_to(dependency(_cond), EEmit)
    assert return_value is emits[0].body


# check — single-shot evaluation


def test_check_satisfied_emits_satisfied():
    """Proves check() emits satisfied when the condition returns satisfied."""

    def _cond():
        return (DependencyState.SATISFIED, None)
        yield

    emit = next(check(_cond))
    assert isinstance(emit, EEmit)
    assert emit.body[0] == "satisfied"


def test_check_satisfied_returns_satisfied_tuple():
    """Proves check() returns a satisfied tuple when the condition is met."""

    def _cond():
        return (DependencyState.SATISFIED, None)
        yield

    _, result = drain_to(check(_cond))
    assert result[0] == "satisfied"


def test_check_unsatisfied_emits_impossible():
    """Proves check() emits impossible when the condition returns unsatisfied."""

    def _cond():
        return (DependencyState.UNSATISFIED, None)
        yield

    emit = next(check(_cond))
    assert isinstance(emit, EEmit)
    assert emit.body[0] == "impossible"


def test_check_unsatisfied_returns_impossible_tuple():
    """Proves check() returns an impossible tuple when the condition is unsatisfied."""

    def _cond():
        return (DependencyState.UNSATISFIED, None)
        yield

    _, result = drain_to(check(_cond))
    assert result[0] == "impossible"


def test_check_unsatisfied_metadata_passes_through():
    """Proves check() preserves condition metadata when mapping unsatisfied to impossible."""

    def _cond():
        return (DependencyState.UNSATISFIED, {"hint": "try later"})
        yield

    emit = next(check(_cond))
    assert emit.body[0] == "impossible"
    assert emit.body[1] == {"hint": "try later"}


def test_check_impossible_condition_emits_impossible():
    """Proves check() emits impossible when the condition returns impossible."""

    def _cond():
        return (DependencyState.IMPOSSIBLE, {"reason": "hard stop"})
        yield

    emit = next(check(_cond))
    assert emit.body[0] == "impossible"


def test_check_impossible_metadata_preserved():
    """Proves check() preserves metadata from an impossible ConditionResult."""

    def _cond():
        return (DependencyState.IMPOSSIBLE, {"reason": "hard stop"})
        yield

    emit = next(check(_cond))
    assert emit.body[1] == {"reason": "hard stop"}


def test_check_does_not_retry_on_unsatisfied():
    """Proves check() never yields ESleep — it stops after one evaluation."""

    calls = 0

    def _cond():
        nonlocal calls
        calls += 1
        return (DependencyState.UNSATISFIED, None)
        yield

    drain_to(check(_cond))
    assert calls == 1


def test_check_metadata_attached_on_satisfied():
    """Proves metadata from a satisfied ConditionResult is attached to the satisfied body."""

    def _cond():
        return (DependencyState.SATISFIED, {"key": "val"})
        yield

    emit = next(check(_cond))
    assert emit.body[1] == {"key": "val"}


def test_check_condition_effects_pass_through():
    """Proves effects yielded by the condition are forwarded to the caller."""

    def _cond():
        yield ESleep(ms=7)
        return (DependencyState.SATISFIED, None)

    effects, _ = drain_to(check(_cond))
    assert any(isinstance(e, ESleep) and e.ms == 7 for e in effects)
    emits = [e for e in effects if isinstance(e, EEmit)]
    assert any(e.body[0] == "satisfied" for e in emits)
