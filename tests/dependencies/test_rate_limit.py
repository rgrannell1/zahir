# Unit tests for rate_limit_condition and rate_limit_dependency.
from collections.abc import Generator
from datetime import UTC, datetime
from typing import Any

import pytest
from bookman.events import Event
from tertius import EEmit, ESleep

from tests.shared import drain_to
from zahir.core.coeffects import CurrentTime
from zahir.core.commons.zahir_types import ConditionResult
from zahir.core.dependencies.rate_limit import rate_limit_condition
from zahir.core.effects import EAcquire, EGetState, EReleaseSlot, ESetState

_NAME = "fetch"
_MIN_SECONDS = 1.0
_NOW = 1_000_000.0  # arbitrary fixed timestamp
_NOW_TIME = datetime.fromtimestamp(_NOW, tz=UTC)
_LABEL = f"rate_limit '{_NAME}' ({_MIN_SECONDS}s)"


def _make_gen(min_seconds: float = _MIN_SECONDS) -> Generator[Any, Any, ConditionResult]:
    label = f"rate_limit '{_NAME}' ({min_seconds}s)"
    return rate_limit_condition(_NAME, min_seconds, label)


def test_first_yield_is_eacquire():
    """Proves rate_limit_condition yields EAcquire with limit=1 as its first effect."""

    gen = _make_gen()
    effect = next(gen)
    assert isinstance(effect, EAcquire)
    assert effect.name == f"rate_limit:{_NAME}"
    assert effect.limit == 1


def test_elapsed_time_comes_from_context():
    """Proves the condition requests its wall-clock time as context."""

    gen = _make_gen()
    next(gen)
    get_state_effect = gen.send(True)
    assert isinstance(get_state_effect, EGetState)

    current_time = gen.send(str(_NOW - _MIN_SECONDS))
    assert isinstance(current_time, CurrentTime)


def test_slot_busy_returns_unsatisfied():
    """Proves the condition returns unsatisfied immediately when the mutex slot is taken."""

    gen = _make_gen()
    next(gen)  # EAcquire
    _, result = drain_to(gen, responses={EAcquire: False})
    assert result == ("unsatisfied", {"name": _NAME, "reason": "slot busy"})


def test_no_prior_run_satisfies_immediately():
    """Proves the condition is satisfied on first run (no last_at → elapsed is very large)."""

    gen = _make_gen()
    responses = {EAcquire: True, EGetState: None, CurrentTime: _NOW_TIME, ESetState: None}
    _effects, result = drain_to(gen, responses=responses)

    assert result[0] == "satisfied"
    assert isinstance(result[1]["elapsed"], float)
    assert result[1]["elapsed"] >= _MIN_SECONDS


def test_gate_released_after_satisfaction():
    """Proves the mutex slot is released by the condition itself, not held until job exit."""

    gen = _make_gen()
    responses = {EAcquire: True, EGetState: None, CurrentTime: _NOW_TIME, ESetState: None}
    effects, result = drain_to(gen, responses=responses)

    assert result[0] == "satisfied"
    stamps = [effect for effect in effects if isinstance(effect, ESetState)]
    assert [stamp.value for stamp in stamps] == [str(_NOW)]
    releases = [effect for effect in effects if isinstance(effect, EReleaseSlot)]
    assert [release.name for release in releases] == [f"rate_limit:{_NAME}"]

    stamp_idx = next(idx for idx, effect in enumerate(effects) if isinstance(effect, ESetState))
    release_idx = effects.index(releases[0])
    assert release_idx > stamp_idx


def test_no_release_when_slot_was_busy():
    """Proves an unsatisfied probe (slot busy) releases nothing it never held."""

    gen = _make_gen()
    next(gen)  # EAcquire
    effects, _ = drain_to(gen, responses={EAcquire: False})
    assert not any(isinstance(effect, EReleaseSlot) for effect in effects)


def test_elapsed_sufficient_satisfies_without_sleep():
    """Proves the condition satisfies without sleeping when enough time has already elapsed."""

    last_at = str(_NOW - _MIN_SECONDS - 1.0)  # well past the threshold

    gen = _make_gen()
    responses = {EAcquire: True, EGetState: last_at, CurrentTime: _NOW_TIME, ESetState: None}
    effects, result = drain_to(gen, responses=responses)

    sleep_effects = [eff for eff in effects if isinstance(eff, ESleep)]
    assert sleep_effects == [], "expected no ESleep when elapsed is already sufficient"
    assert result[0] == "satisfied"


def test_elapsed_too_short_emits_waiting_point_then_sleeps():
    """Proves emitting WAITING point when elapsed < min_seconds, then yielding ESleep."""

    last_at = str(_NOW - 0.1)  # only 0.1s ago, need 1.0s

    gen = _make_gen()
    next(gen)  # EAcquire
    gen.send(True)  # acquired → EGetState
    current_time = gen.send(last_at)
    assert isinstance(current_time, CurrentTime)
    emit_effect = gen.send(_NOW_TIME)
    sleep_effect = gen.send(None)  # emit done → ESleep

    assert isinstance(emit_effect, EEmit) and isinstance(
        emit_effect.body, Event
    ), "expected EEmit wrapping a bookman Event (waiting point) before ESleep"
    assert isinstance(sleep_effect, ESleep)
    assert sleep_effect.ms == pytest.approx(900, abs=10)


def test_sleep_duration_covers_remaining_gap():
    """Proves ESleep duration is the remaining time until min_seconds has elapsed."""

    cases = [
        {"elapsed": 0.0, "min_seconds": 1.0, "expected_ms": 1000},
        {"elapsed": 0.5, "min_seconds": 1.0, "expected_ms": 500},
        {"elapsed": 0.9, "min_seconds": 1.0, "expected_ms": 100},
        {"elapsed": 0.0, "min_seconds": 0.3, "expected_ms": 300},
    ]

    for case in cases:
        last_at = str(_NOW - case["elapsed"])
        gen = _make_gen(case["min_seconds"])
        next(gen)
        gen.send(True)
        gen.send(last_at)
        gen.send(_NOW_TIME)
        sleep_effect = gen.send(None)  # emit done → ESleep

        assert isinstance(sleep_effect, ESleep), f"expected ESleep for elapsed={case['elapsed']}"
        expected_ms = case["expected_ms"]
        within_tolerance = abs(sleep_effect.ms - expected_ms) <= 10
        assert within_tolerance, (
            f"elapsed={case['elapsed']}s: expected ~{expected_ms}ms sleep, "
            f"got {sleep_effect.ms}ms"
        )


def test_satisfied_after_sleep_re_reads_state():
    """Proves that after ESleep the condition reads state again before checking elapsed."""

    last_at_initial = str(_NOW - 0.1)  # 0.1s ago → need to sleep 0.9s

    gen = _make_gen()
    next(gen)  # EAcquire
    gen.send(True)  # acquired → EGetState
    gen.send(last_at_initial)
    gen.send(_NOW_TIME)  # elapsed=0.1 → EEmit waiting point
    gen.send(None)  # emit done → ESleep
    get_state_effect = gen.send(None)  # sleep done → re-read EGetState

    assert isinstance(get_state_effect, EGetState), "expected re-read of state after sleep"
