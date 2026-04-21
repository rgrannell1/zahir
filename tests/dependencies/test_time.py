import pytest
import time_machine

from tertius import EEmit, ESleep

from zahir.core.dependencies.time import time_dependency
from tests.shared import FUTURE, NOW, PAST


@time_machine.travel(NOW, tick=False)
def test_no_constraints_emits_satisfied():
    """Proves time_dependency with no constraints is immediately satisfied."""

    emit = next(time_dependency(before=None, after=None))
    assert isinstance(emit, EEmit)
    assert emit.body[0] == "satisfied"


@time_machine.travel(NOW, tick=False)
def test_before_not_yet_passed_emits_satisfied():
    """Proves a future before constraint does not block satisfaction."""

    emit = next(time_dependency(before=FUTURE, after=None))
    assert emit.body[0] == "satisfied"


@time_machine.travel(NOW, tick=False)
def test_before_passed_emits_impossible():
    """Proves a past before constraint emits impossible."""

    emit = next(time_dependency(before=PAST, after=None))
    assert emit.body[0] == "impossible"


@time_machine.travel(NOW, tick=False)
def test_after_not_yet_reached_yields_sleep_then_satisfied():
    """Proves a future after constraint sleeps the exact remaining duration before satisfying."""

    gen = time_dependency(before=None, after=FUTURE)
    sleep = next(gen)

    assert isinstance(sleep, ESleep)
    assert sleep.ms == int((FUTURE - NOW).total_seconds() * 1000)

    emit = gen.send(None)
    assert emit.body[0] == "satisfied"


@time_machine.travel(NOW, tick=False)
def test_after_already_passed_emits_satisfied():
    """Proves a past after constraint is immediately satisfied."""

    emit = next(time_dependency(before=None, after=PAST))
    assert emit.body[0] == "satisfied"


@time_machine.travel(NOW, tick=False)
def test_before_and_after_both_satisfied():
    """Proves both constraints satisfied when now is within the window."""

    emit = next(time_dependency(before=FUTURE, after=PAST))
    assert emit.body[0] == "satisfied"


@time_machine.travel(NOW, tick=False)
def test_impossible_includes_timestamps():
    """Proves the impossible reason includes the violated before timestamp."""

    emit = next(time_dependency(before=PAST, after=None))
    assert emit.body[0] == "impossible"
    assert PAST.isoformat() in emit.body[1]


# return values


@time_machine.travel(NOW, tick=False)
def test_impossible_returns_tuple_as_generator_value():
    """Proves the generator returns the impossible tuple as its StopIteration value."""

    gen = time_dependency(before=PAST)
    emit = next(gen)
    with pytest.raises(StopIteration) as exc:
        next(gen)
    assert exc.value.value is emit.body


@time_machine.travel(NOW, tick=False)
def test_satisfied_returns_tuple_as_generator_value():
    """Proves the generator returns the satisfied tuple as its StopIteration value."""

    gen = time_dependency(before=FUTURE)
    emit = next(gen)
    with pytest.raises(StopIteration) as exc:
        next(gen)
    assert exc.value.value is emit.body
