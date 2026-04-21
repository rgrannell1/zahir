from datetime import timedelta
from unittest.mock import patch

import pytest
import time_machine

from tertius import EEmit, ESleep

from zahir.core.dependencies.resources import resource_dependency
from tests.shared import NOW


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

    with patch("zahir.core.dependencies.resources._get_usage", _high_usage):
        assert isinstance(next(resource_dependency("cpu", max_percent=50.0)), ESleep)


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
            next(gen)  # ESleep

        with time_machine.travel(NOW + timedelta(seconds=2), tick=False):
            emit = next(gen)

    assert isinstance(emit, EEmit)
    assert emit.body[0] == "impossible"
    assert "cpu" in emit.body[1]


def test_timeout_reason_includes_resource_and_duration():
    """Proves the impossible reason includes the resource type and timeout duration."""

    with patch("zahir.core.dependencies.resources._get_usage", _high_usage):
        with time_machine.travel(NOW, tick=False):
            gen = resource_dependency("memory", max_percent=50.0, timeout=30.0)
            next(gen)

        with time_machine.travel(NOW + timedelta(seconds=60), tick=False):
            emit = next(gen)

    assert emit.body[0] == "impossible"
    assert "memory" in emit.body[1]
    assert "30" in emit.body[1]


def test_high_then_low_usage_emits_satisfied():
    """Proves the dependency satisfies once usage drops below the limit."""

    calls = iter([_high_usage, _low_usage])

    with patch(
        "zahir.core.dependencies.resources._get_usage", lambda r: next(calls)(r)
    ):
        with time_machine.travel(NOW, tick=False):
            gen = resource_dependency("cpu", max_percent=50.0)
            assert isinstance(next(gen), ESleep)
            emit = next(gen)
            assert emit.body[0] == "satisfied"


# return values


@time_machine.travel(NOW, tick=False)
def test_satisfied_returns_tuple_as_generator_value():
    """Proves the generator returns the satisfied tuple as its StopIteration value."""

    with patch("zahir.core.dependencies.resources._get_usage", _low_usage):
        gen = resource_dependency("cpu", max_percent=50.0)
        emit = next(gen)
    with pytest.raises(StopIteration) as exc:
        next(gen)
    assert exc.value.value is emit.body


def test_impossible_returns_tuple_as_generator_value():
    """Proves the generator returns the impossible tuple as its StopIteration value."""

    with patch("zahir.core.dependencies.resources._get_usage", _high_usage):
        with time_machine.travel(NOW, tick=False):
            gen = resource_dependency("cpu", max_percent=50.0, timeout=1.0)
            next(gen)

        with time_machine.travel(NOW + timedelta(seconds=2), tick=False):
            emit = next(gen)

    with pytest.raises(StopIteration) as exc:
        next(gen)
    assert exc.value.value is emit.body
