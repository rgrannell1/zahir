from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import time_machine

from dependencies.resources import resource_dependency
from effects import EImpossible, ESatisfied
from tertius import ESleep


NOW = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)


def _low_usage(_resource):
    return 10.0


def _high_usage(_resource):
    return 90.0


@time_machine.travel(NOW, tick=False)
def test_cpu_below_limit_yields_satisfied():
    """Proves cpu usage below max_percent yields ESatisfied."""

    with patch("dependencies.resources._get_usage", _low_usage):
        gen = resource_dependency("cpu", max_percent=50.0)
        assert isinstance(next(gen), ESatisfied)


@time_machine.travel(NOW, tick=False)
def test_memory_below_limit_yields_satisfied():
    """Proves memory usage below max_percent yields ESatisfied."""

    with patch("dependencies.resources._get_usage", _low_usage):
        gen = resource_dependency("memory", max_percent=50.0)
        assert isinstance(next(gen), ESatisfied)


@time_machine.travel(NOW, tick=False)
def test_usage_above_limit_yields_sleep():
    """Proves usage exceeding max_percent yields ESleep."""

    with patch("dependencies.resources._get_usage", _high_usage):
        gen = resource_dependency("cpu", max_percent=50.0)
        assert isinstance(next(gen), ESleep)


@time_machine.travel(NOW, tick=False)
def test_usage_at_limit_yields_satisfied():
    """Proves usage exactly at max_percent is considered within limit."""

    with patch("dependencies.resources._get_usage", lambda _: 50.0):
        gen = resource_dependency("cpu", max_percent=50.0)
        assert isinstance(next(gen), ESatisfied)


@time_machine.travel(NOW, tick=False)
def test_satisfied_metadata_includes_resource_and_limit():
    """Proves ESatisfied metadata contains the resource type and max_percent."""

    with patch("dependencies.resources._get_usage", _low_usage):
        gen = resource_dependency("cpu", max_percent=75.0)
        effect = next(gen)

    assert effect.metadata["resource"] == "cpu"
    assert effect.metadata["max_percent"] == 75.0


def test_timeout_yields_impossible():
    """Proves exceeding timeout yields EImpossible."""

    with patch("dependencies.resources._get_usage", _high_usage):
        with time_machine.travel(NOW, tick=False):
            gen = resource_dependency("cpu", max_percent=50.0, timeout=1.0)
            next(gen)  # ESleep

        with time_machine.travel(NOW + timedelta(seconds=2), tick=False):
            effect = next(gen)

    assert isinstance(effect, EImpossible)
    assert "cpu" in effect.reason


def test_timeout_reason_includes_resource_and_duration():
    """Proves EImpossible reason includes the resource type and timeout duration."""

    with patch("dependencies.resources._get_usage", _high_usage):
        with time_machine.travel(NOW, tick=False):
            gen = resource_dependency("memory", max_percent=50.0, timeout=30.0)
            next(gen)

        with time_machine.travel(NOW + timedelta(seconds=60), tick=False):
            effect = next(gen)

    assert "memory" in effect.reason
    assert "30" in effect.reason


def test_high_then_low_usage_yields_satisfied():
    """Proves the dependency satisfies once usage drops below the limit."""

    calls = iter([_high_usage, _low_usage])

    with patch("dependencies.resources._get_usage", lambda r: next(calls)(r)):
        with time_machine.travel(NOW, tick=False):
            gen = resource_dependency("cpu", max_percent=50.0)
            assert isinstance(next(gen), ESleep)
            assert isinstance(next(gen), ESatisfied)
