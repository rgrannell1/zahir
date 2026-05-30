from datetime import timedelta
from unittest.mock import patch

import time_machine
from tertius import EEmit, ESleep

from tests.shared import NOW, drain_to
from zahir.core.dependencies.resources import (
    check_resource_dependency,
    resource_dependency,
)


def _low_usage(_resource):
    return 10.0


def _high_usage(_resource):
    return 90.0


@time_machine.travel(NOW, tick=False)
def test_cpu_below_limit_emits_satisfied():
    """Proves cpu usage below max_percent emits satisfied."""

    with patch("zahir.core.dependencies.resources._get_usage", _low_usage):
        emit = next(resource_dependency("cpu", max_percent=50.0))
    assert isinstance(emit, EEmit)
    assert emit.body[0] == "satisfied"


@time_machine.travel(NOW, tick=False)
def test_memory_below_limit_emits_satisfied():
    """Proves memory usage below max_percent emits satisfied."""

    with patch("zahir.core.dependencies.resources._get_usage", _low_usage):
        emit = next(resource_dependency("memory", max_percent=50.0))
    assert emit.body[0] == "satisfied"


@time_machine.travel(NOW, tick=False)
def test_usage_above_limit_yields_sleep():
    """Proves usage exceeding max_percent yields ESleep."""

    calls = iter([_high_usage, _low_usage])
    with patch("zahir.core.dependencies.resources._get_usage", lambda r: next(calls)(r)):
        effects, _ = drain_to(resource_dependency("cpu", max_percent=50.0))
    assert any(isinstance(e, ESleep) for e in effects)


@time_machine.travel(NOW, tick=False)
def test_usage_at_limit_emits_satisfied():
    """Proves usage exactly at max_percent is considered within limit."""

    with patch("zahir.core.dependencies.resources._get_usage", lambda _: 50.0):
        emit = next(resource_dependency("cpu", max_percent=50.0))
    assert emit.body[0] == "satisfied"


@time_machine.travel(NOW, tick=False)
def test_satisfied_metadata_includes_resource_and_limit():
    """Proves the satisfied body contains the resource type and max_percent."""

    with patch("zahir.core.dependencies.resources._get_usage", _low_usage):
        emit = next(resource_dependency("cpu", max_percent=75.0))

    assert emit.body[0] == "satisfied"
    assert emit.body[1]["resource"] == "cpu"
    assert emit.body[1]["max_percent"] == 75.0


def test_timeout_emits_impossible():
    """Proves exceeding timeout emits impossible."""

    with patch("zahir.core.dependencies.resources._get_usage", _high_usage):
        with time_machine.travel(NOW, tick=False):
            gen = resource_dependency("cpu", max_percent=50.0, timeout=1.0)
            next(gen)  # advance through one retry: EEmit(waiting)
            next(gen)  # advance through one retry: ESleep

        with time_machine.travel(NOW + timedelta(seconds=2), tick=False):
            emits, _ = drain_to(gen, EEmit)

    assert emits[0].body[0] == "impossible"
    assert "cpu" in emits[0].body[1]["reason"]


def test_timeout_reason_includes_resource_and_duration():
    """Proves the impossible reason includes the resource type and timeout duration."""

    with patch("zahir.core.dependencies.resources._get_usage", _high_usage):
        with time_machine.travel(NOW, tick=False):
            gen = resource_dependency("memory", max_percent=50.0, timeout=30.0)
            next(gen)  # advance through one retry: EEmit(waiting)
            next(gen)  # advance through one retry: ESleep

        with time_machine.travel(NOW + timedelta(seconds=60), tick=False):
            emits, _ = drain_to(gen, EEmit)

    assert emits[0].body[0] == "impossible"
    assert "memory" in emits[0].body[1]["reason"]
    assert "30" in emits[0].body[1]["reason"]


def test_high_then_low_usage_emits_satisfied():
    """Proves the dependency satisfies once usage drops below the limit."""

    calls = iter([_high_usage, _low_usage])

    with patch(
        "zahir.core.dependencies.resources._get_usage",
        lambda resource: next(calls)(resource),
    ), time_machine.travel(NOW, tick=False):
        effects, _ = drain_to(resource_dependency("cpu", max_percent=50.0))

    assert any(isinstance(e, ESleep) for e in effects)
    emits = [e for e in effects if isinstance(e, EEmit)]
    assert any(e.body[0] == "satisfied" for e in emits)


# return values


@time_machine.travel(NOW, tick=False)
def test_satisfied_returns_tuple_as_generator_value():
    """Proves the generator returns the satisfied tuple as its StopIteration value."""

    with patch("zahir.core.dependencies.resources._get_usage", _low_usage):
        emits, return_value = drain_to(resource_dependency("cpu", max_percent=50.0), EEmit)
    assert return_value is emits[0].body


def test_impossible_returns_tuple_as_generator_value():
    """Proves the generator returns the impossible tuple as its StopIteration value."""

    with patch("zahir.core.dependencies.resources._get_usage", _high_usage):
        with time_machine.travel(NOW, tick=False):
            gen = resource_dependency("cpu", max_percent=50.0, timeout=1.0)
            next(gen)  # advance through one retry: EEmit(waiting)
            next(gen)  # advance through one retry: ESleep

        with time_machine.travel(NOW + timedelta(seconds=2), tick=False):
            emits, return_value = drain_to(gen, EEmit)

    assert return_value is emits[0].body


# check_resource_dependency


def test_check_resource_low_usage_emits_satisfied():
    """Proves check_resource_dependency emits satisfied when usage is within limit."""

    with patch("zahir.core.dependencies.resources._get_usage", _low_usage):
        emit = next(check_resource_dependency("cpu", max_percent=50.0))
    assert emit.body[0] == "satisfied"


def test_check_resource_high_usage_emits_impossible():
    """Proves check_resource_dependency emits impossible when usage exceeds the limit."""

    with patch("zahir.core.dependencies.resources._get_usage", _high_usage):
        emit = next(check_resource_dependency("cpu", max_percent=50.0))
    assert emit.body[0] == "impossible"


def test_check_resource_high_usage_does_not_sleep():
    """Proves check_resource_dependency never yields ESleep on high usage."""

    calls = 0

    def _counting_usage(_r):
        nonlocal calls
        calls += 1
        return 90.0

    with patch("zahir.core.dependencies.resources._get_usage", _counting_usage):
        drain_to(check_resource_dependency("cpu", max_percent=50.0))

    assert calls == 1


def test_check_resource_metadata_contains_resource_type():
    """Proves the resource type appears in the impossible metadata when check fails."""

    with patch("zahir.core.dependencies.resources._get_usage", _high_usage):
        emit = next(check_resource_dependency("memory", max_percent=50.0))
    assert emit.body[1]["resource"] == "memory"
