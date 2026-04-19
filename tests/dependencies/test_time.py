from datetime import UTC, datetime, timedelta

import time_machine

from dependencies.time import time_dependency
from effects import EImpossible, ESatisfied
from tertius import ESleep


NOW = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
PAST = NOW - timedelta(hours=1)
FUTURE = NOW + timedelta(hours=1)


@time_machine.travel(NOW, tick=False)
def test_no_constraints_yields_satisfied():
    """Proves time_dependency with no constraints is immediately satisfied."""

    gen = time_dependency(before=None, after=None)
    assert isinstance(next(gen), ESatisfied)


@time_machine.travel(NOW, tick=False)
def test_before_not_yet_passed_yields_satisfied():
    """Proves a future before constraint does not block satisfaction."""

    gen = time_dependency(before=FUTURE, after=None)
    assert isinstance(next(gen), ESatisfied)


@time_machine.travel(NOW, tick=False)
def test_before_passed_yields_impossible():
    """Proves a past before constraint yields EImpossible."""

    gen = time_dependency(before=PAST, after=None)
    assert isinstance(next(gen), EImpossible)


@time_machine.travel(NOW, tick=False)
def test_after_not_yet_reached_yields_sleep_then_satisfied():
    """Proves a future after constraint sleeps the exact remaining duration before satisfying."""

    gen = time_dependency(before=None, after=FUTURE)
    sleep = next(gen)

    assert isinstance(sleep, ESleep)
    assert sleep.ms == int((FUTURE - NOW).total_seconds() * 1000)

    assert isinstance(gen.send(None), ESatisfied)


@time_machine.travel(NOW, tick=False)
def test_after_already_passed_yields_satisfied():
    """Proves a past after constraint is immediately satisfied."""

    gen = time_dependency(before=None, after=PAST)
    assert isinstance(next(gen), ESatisfied)


@time_machine.travel(NOW, tick=False)
def test_before_and_after_both_satisfied():
    """Proves both constraints satisfied when now is within the window."""

    gen = time_dependency(before=FUTURE, after=PAST)
    assert isinstance(next(gen), ESatisfied)


@time_machine.travel(NOW, tick=False)
def test_impossible_includes_timestamps():
    """Proves EImpossible reason includes the violated before timestamp."""

    gen = time_dependency(before=PAST, after=None)
    effect = next(gen)

    assert isinstance(effect, EImpossible)
    assert PAST.isoformat() in effect.reason
