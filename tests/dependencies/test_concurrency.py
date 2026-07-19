import time
from unittest.mock import patch

import time_machine
from tertius import EEmit, ESleep

from tests.shared import NOW, drain_to
from zahir.core.dependencies.concurrency import concurrency_dependency
from zahir.core.effects import EAcquire

# concurrency_dependency — first yield


def test_slot_available_yields_esatisfied():
    """Proves yielding True for EAcquire immediately emits a satisfied result."""

    gen = concurrency_dependency("workers", limit=4)
    assert isinstance(next(gen), EAcquire)
    emit = gen.send(True)
    assert isinstance(emit, EEmit)
    assert emit.body[0] == "satisfied"


def test_slot_unavailable_yields_esleep():
    """Proves yielding False for EAcquire causes the dependency to sleep and retry."""

    gen = concurrency_dependency("workers", limit=4)
    assert isinstance(next(gen), EAcquire)
    gen.send(False)  # EEmit(waiting) telemetry before sleep
    assert isinstance(next(gen), ESleep)


def test_acquire_effect_carries_name_and_limit():
    """Proves the EAcquire effect has the correct name and limit."""

    gen = concurrency_dependency("workers", limit=4)
    effect = next(gen)
    assert isinstance(effect, EAcquire)
    assert effect.name == "workers"
    assert effect.limit == 4


def test_retry_after_denied_yields_eacquire_again():
    """Proves the dependency loops and retries with another EAcquire after denial."""

    gen = concurrency_dependency("workers", limit=4)
    next(gen)  # EAcquire
    gen.send(False)  # EEmit(waiting)
    next(gen)  # ESleep
    assert isinstance(next(gen), EAcquire)


# concurrency_dependency — satisfied metadata


def test_satisfied_metadata_includes_name_and_limit():
    """Proves the satisfied emit body contains the slot name and limit."""

    gen = concurrency_dependency("workers", limit=4)
    next(gen)
    emit = gen.send(True)
    assert emit.body[0] == "satisfied"
    assert emit.body[1]["name"] == "workers"
    assert emit.body[1]["limit"] == 4


# concurrency_dependency — timeout


@time_machine.travel(NOW, tick=False)
def test_timeout_yields_eimpossible():
    """Proves exceeding timeout_ms emits an impossible result."""

    gen = concurrency_dependency("workers", limit=4, timeout_ms=1000)
    next(gen)  # EAcquire
    gen.send(False)  # EEmit(waiting)
    next(gen)  # ESleep

    with patch("time.monotonic", return_value=time.monotonic() + 2.0):
        emits, _ = drain_to(gen, EEmit)

    assert emits[0].body[0] == "impossible"


@time_machine.travel(NOW, tick=False)
def test_timeout_reason_includes_name_and_duration():
    """Proves the impossible reason includes the slot name and timeout duration."""

    gen = concurrency_dependency("workers", limit=4, timeout_ms=5000)
    next(gen)
    gen.send(False)  # EEmit(waiting)
    next(gen)  # ESleep

    with patch("time.monotonic", return_value=time.monotonic() + 10.0):
        emits, _ = drain_to(gen, EEmit)

    assert emits[0].body[0] == "impossible"
    assert "workers" in emits[0].body[1]["reason"]
    assert "5000" in emits[0].body[1]["reason"]


# concurrency_dependency — return values


def test_satisfied_returns_tuple_as_generator_value():
    """Proves the generator returns the satisfied tuple as its StopIteration value."""

    gen = concurrency_dependency("workers", limit=4)
    next(gen)
    emit = gen.send(True)
    _, return_value = drain_to(gen)
    assert return_value is emit.body


@time_machine.travel(NOW, tick=False)
def test_impossible_returns_tuple_as_generator_value():
    """Proves the generator returns the impossible tuple as its StopIteration value."""

    gen = concurrency_dependency("workers", limit=4, timeout_ms=1000)
    next(gen)
    gen.send(False)  # EEmit(waiting)
    next(gen)  # ESleep

    with patch("time.monotonic", return_value=time.monotonic() + 2.0):
        emits, return_value = drain_to(gen, EEmit)

    assert return_value is emits[0].body
