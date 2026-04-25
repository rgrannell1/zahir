from datetime import timedelta

import pytest
import time_machine

from tertius import EEmit, ESleep

from zahir.core.dependencies.semaphore import check_semaphore_dependency, semaphore_dependency
from zahir.core.effects import EGetSemaphore
from tests.shared import NOW


@time_machine.travel(NOW, tick=False)
def test_first_yield_is_esignal():
    """Proves semaphore_dependency first probes the semaphore via EGetSemaphore."""

    gen = semaphore_dependency("db")
    effect = next(gen)
    assert isinstance(effect, EGetSemaphore)
    assert effect.name == "db"


@time_machine.travel(NOW, tick=False)
def test_satisfied_state_emits_satisfied():
    """Proves a satisfied signal emits a satisfied result."""

    gen = semaphore_dependency("db")
    next(gen)
    emit = gen.send("satisfied")
    assert isinstance(emit, EEmit)
    assert emit.body[0] == "satisfied"


@time_machine.travel(NOW, tick=False)
def test_impossible_state_emits_impossible():
    """Proves an impossible signal emits an impossible result."""

    gen = semaphore_dependency("db")
    next(gen)
    emit = gen.send("impossible")
    assert isinstance(emit, EEmit)
    assert emit.body[0] == "impossible"


@time_machine.travel(NOW, tick=False)
def test_unsatisfied_state_yields_sleep():
    """Proves an unsatisfied signal yields ESleep before probing again."""

    gen = semaphore_dependency("db")
    next(gen)
    assert isinstance(gen.send("unsatisfied"), ESleep)


@time_machine.travel(NOW, tick=False)
def test_unsatisfied_then_satisfied_emits_satisfied():
    """Proves the dependency re-probes after sleep and satisfies on next signal."""

    gen = semaphore_dependency("db")
    next(gen)  # EGetSemaphore
    gen.send("unsatisfied")  # ESleep
    signal = next(gen)  # next EGetSemaphore
    assert isinstance(signal, EGetSemaphore)
    emit = gen.send("satisfied")
    assert emit.body[0] == "satisfied"


def test_timeout_emits_impossible():
    """Proves exceeding timeout_ms emits impossible on the next probe."""

    with time_machine.travel(NOW, tick=False):
        gen = semaphore_dependency("db", timeout_ms=1000)
        next(gen)  # EGetSemaphore
        gen.send("unsatisfied")  # ESleep

    with time_machine.travel(NOW + timedelta(seconds=2), tick=False):
        emit = next(gen)

    assert isinstance(emit, EEmit)
    assert emit.body[0] == "impossible"
    assert "db" in emit.body[1]


def test_timeout_reason_includes_name_and_duration():
    """Proves the impossible reason includes the semaphore name and timeout duration."""

    with time_machine.travel(NOW, tick=False):
        gen = semaphore_dependency("my-semaphore", timeout_ms=5000)
        next(gen)
        gen.send("unsatisfied")

    with time_machine.travel(NOW + timedelta(seconds=10), tick=False):
        emit = next(gen)

    assert emit.body[0] == "impossible"
    assert "my-semaphore" in emit.body[1]
    assert "5000" in emit.body[1]


@time_machine.travel(NOW, tick=False)
def test_no_timeout_polls_indefinitely():
    """Proves no timeout_ms means the dependency never expires on its own."""

    gen = semaphore_dependency("db")
    for _ in range(10):
        next(gen)  # EGetSemaphore
        gen.send("unsatisfied")  # ESleep


# return values


@time_machine.travel(NOW, tick=False)
def test_satisfied_returns_tuple_as_generator_value():
    """Proves the generator returns the satisfied tuple as its StopIteration value."""

    gen = semaphore_dependency("db")
    next(gen)
    emit = gen.send("satisfied")
    with pytest.raises(StopIteration) as exc:
        next(gen)
    assert exc.value.value is emit.body


@time_machine.travel(NOW, tick=False)
def test_impossible_state_returns_tuple_as_generator_value():
    """Proves the generator returns the impossible tuple as its StopIteration value when signal is impossible."""

    gen = semaphore_dependency("db")
    next(gen)
    emit = gen.send("impossible")
    with pytest.raises(StopIteration) as exc:
        next(gen)
    assert exc.value.value is emit.body


# check_semaphore_dependency


def test_check_semaphore_satisfied_emits_satisfied():
    """Proves check_semaphore_dependency emits satisfied when the semaphore is satisfied."""

    gen = check_semaphore_dependency("db")
    next(gen)  # EGetSemaphore
    emit = gen.send("satisfied")
    assert emit.body[0] == "satisfied"


def test_check_semaphore_impossible_emits_impossible():
    """Proves check_semaphore_dependency emits impossible when the semaphore is impossible."""

    gen = check_semaphore_dependency("db")
    next(gen)
    emit = gen.send("impossible")
    assert emit.body[0] == "impossible"


def test_check_semaphore_unsatisfied_emits_impossible():
    """Proves check_semaphore_dependency emits impossible when the semaphore is unsatisfied."""

    gen = check_semaphore_dependency("db")
    next(gen)
    emit = gen.send("unsatisfied")
    assert emit.body[0] == "impossible"


def test_check_semaphore_unsatisfied_does_not_sleep():
    """Proves check_semaphore_dependency never yields ESleep on unsatisfied state."""

    gen = check_semaphore_dependency("db")
    next(gen)  # EGetSemaphore
    effect = gen.send("unsatisfied")
    assert isinstance(effect, EEmit)  # EEmit(impossible), not ESleep


def test_check_semaphore_label_in_impossible_reason():
    """Proves the semaphore name appears in the impossible reason."""

    gen = check_semaphore_dependency("my-gate")
    next(gen)
    emit = gen.send("unsatisfied")
    assert "my-gate" in emit.body[1]


def test_impossible_timeout_returns_tuple_as_generator_value():
    """Proves the generator returns the impossible tuple as its StopIteration value on timeout."""

    with time_machine.travel(NOW, tick=False):
        gen = semaphore_dependency("db", timeout_ms=1000)
        next(gen)
        gen.send("unsatisfied")

    with time_machine.travel(NOW + timedelta(seconds=2), tick=False):
        emit = next(gen)

    with pytest.raises(StopIteration) as exc:
        next(gen)
    assert exc.value.value is emit.body
