import pytest
import time_machine

from tertius import EEmit, ESleep

from zahir.core.dependencies.time import check_time_dependency, time_dependency
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
def test_after_not_yet_reached_yields_sleep():
    """Proves a future after constraint yields ESleep via the polling loop."""

    gen = time_dependency(before=None, after=FUTURE)
    assert isinstance(next(gen), ESleep)


def test_after_not_yet_reached_satisfies_once_time_arrives():
    """Proves the dependency satisfies once the after time has passed."""

    with time_machine.travel(NOW, tick=False):
        gen = time_dependency(before=None, after=FUTURE)
        next(gen)  # ESleep from poll loop

    with time_machine.travel(FUTURE, tick=False):
        emit = next(gen)

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


# check_time_dependency


@time_machine.travel(NOW, tick=False)
def test_check_time_no_constraints_emits_satisfied():
    """Proves check_time_dependency with no constraints is immediately satisfied."""

    emit = next(check_time_dependency())
    assert emit.body[0] == "satisfied"


@time_machine.travel(NOW, tick=False)
def test_check_time_before_not_yet_passed_emits_satisfied():
    """Proves check_time_dependency satisfies when before is in the future."""

    emit = next(check_time_dependency(before=FUTURE))
    assert emit.body[0] == "satisfied"


@time_machine.travel(NOW, tick=False)
def test_check_time_before_passed_emits_impossible():
    """Proves check_time_dependency emits impossible when before is in the past."""

    emit = next(check_time_dependency(before=PAST))
    assert emit.body[0] == "impossible"


@time_machine.travel(NOW, tick=False)
def test_check_time_after_not_yet_reached_emits_impossible():
    """Proves check_time_dependency emits impossible when after has not arrived yet."""

    emit = next(check_time_dependency(after=FUTURE))
    assert emit.body[0] == "impossible"


@time_machine.travel(NOW, tick=False)
def test_check_time_after_not_yet_reached_does_not_sleep():
    """Proves check_time_dependency never yields ESleep when after is in the future."""

    gen = check_time_dependency(after=FUTURE)
    effect = next(gen)
    assert isinstance(effect, EEmit)


@time_machine.travel(NOW, tick=False)
def test_check_time_after_already_passed_emits_satisfied():
    """Proves check_time_dependency satisfies when after is in the past."""

    emit = next(check_time_dependency(after=PAST))
    assert emit.body[0] == "satisfied"
