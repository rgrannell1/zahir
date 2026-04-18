from datetime import UTC, datetime, timedelta

import time_machine

from dependencies.semaphore import semaphore_dependency
from effects import EImpossible, ESatisfied, ESignal
from tertius import ESleep


NOW = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)


def _step(gen, send_value=None):
    return gen.send(send_value) if send_value is not None else next(gen)


@time_machine.travel(NOW, tick=False)
def test_first_yield_is_esignal():
    """Proves semaphore_dependency first probes the semaphore via ESignal."""

    gen = semaphore_dependency("db")
    effect = next(gen)
    assert isinstance(effect, ESignal)
    assert effect.name == "db"


@time_machine.travel(NOW, tick=False)
def test_satisfied_state_yields_esatisfied():
    """Proves a satisfied signal yields ESatisfied."""

    gen = semaphore_dependency("db")
    next(gen)
    assert isinstance(gen.send("satisfied"), ESatisfied)


@time_machine.travel(NOW, tick=False)
def test_impossible_state_yields_eimpossible():
    """Proves an impossible signal yields EImpossible."""

    gen = semaphore_dependency("db")
    next(gen)
    assert isinstance(gen.send("impossible"), EImpossible)


@time_machine.travel(NOW, tick=False)
def test_unsatisfied_state_yields_sleep():
    """Proves an unsatisfied signal yields ESleep before probing again."""

    gen = semaphore_dependency("db")
    next(gen)
    assert isinstance(gen.send("unsatisfied"), ESleep)


@time_machine.travel(NOW, tick=False)
def test_unsatisfied_then_satisfied_yields_esatisfied():
    """Proves the dependency re-probes after sleep and satisfies on next signal."""

    gen = semaphore_dependency("db")
    next(gen)              # ESignal
    gen.send("unsatisfied")  # ESleep
    signal = next(gen)     # next ESignal
    assert isinstance(signal, ESignal)
    assert isinstance(gen.send("satisfied"), ESatisfied)


def test_timeout_yields_impossible():
    """Proves exceeding timeout_ms yields EImpossible on the next probe."""

    with time_machine.travel(NOW, tick=False):
        gen = semaphore_dependency("db", timeout_ms=1000)
        next(gen)                # ESignal
        gen.send("unsatisfied")  # ESleep — loops back to timeout check

    with time_machine.travel(NOW + timedelta(seconds=2), tick=False):
        effect = next(gen)       # timeout_at exceeded

    assert isinstance(effect, EImpossible)
    assert "db" in effect.reason


def test_timeout_reason_includes_name_and_duration():
    """Proves EImpossible reason includes the semaphore name and timeout duration."""

    with time_machine.travel(NOW, tick=False):
        gen = semaphore_dependency("my-semaphore", timeout_ms=5000)
        next(gen)
        gen.send("unsatisfied")

    with time_machine.travel(NOW + timedelta(seconds=10), tick=False):
        effect = next(gen)

    assert "my-semaphore" in effect.reason
    assert "5000" in effect.reason


@time_machine.travel(NOW, tick=False)
def test_no_timeout_polls_indefinitely():
    """Proves no timeout_ms means the dependency never expires on its own."""

    gen = semaphore_dependency("db")
    for _ in range(10):
        next(gen)              # ESignal
        gen.send("unsatisfied")  # ESleep
