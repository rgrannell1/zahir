# Unit tests for rate_limit_condition and rate_limit_dependency.
import pytest
from unittest.mock import patch

from bookman.events import Event
from tertius import EEmit, ESleep

from tests.shared import drain_to
from zahir.core.dependencies.rate_limit import rate_limit_condition
from zahir.core.effects import EAcquire, EGetState, ESetState

_NAME = "fetch"
_MIN_SECONDS = 1.0
_NOW = 1_000_000.0  # arbitrary fixed timestamp
_LABEL = f"rate_limit '{_NAME}' ({_MIN_SECONDS}s)"


def _make_gen(min_seconds: float = _MIN_SECONDS) -> object:
    label = f"rate_limit '{_NAME}' ({min_seconds}s)"
    return rate_limit_condition(_NAME, min_seconds, label)


def test_first_yield_is_eacquire():
    """Proves rate_limit_condition yields EAcquire with limit=1 as its first effect."""

    gen = _make_gen()
    effect = next(gen)
    assert isinstance(effect, EAcquire)
    assert effect.name == f"rate_limit:{_NAME}"
    assert effect.limit == 1


def test_slot_busy_returns_unsatisfied():
    """Proves the condition returns unsatisfied immediately when the mutex slot is taken."""

    gen = _make_gen()
    next(gen)  # EAcquire
    _, result = drain_to(gen, responses={EAcquire: False})
    assert result == ("unsatisfied", {"name": _NAME, "reason": "slot busy"})


def test_no_prior_run_satisfies_immediately():
    """Proves the condition is satisfied on first run (no last_at → elapsed is very large)."""

    with patch("zahir.core.dependencies.rate_limit.time") as mock_time:
        mock_time.time.return_value = _NOW
        gen = _make_gen()
        effects, result = drain_to(gen, responses={EAcquire: True, EGetState: None, ESetState: None})

    assert result[0] == "satisfied"
    assert isinstance(result[1]["elapsed"], float)
    assert result[1]["elapsed"] >= _MIN_SECONDS


def test_elapsed_sufficient_satisfies_without_sleep():
    """Proves the condition satisfies without sleeping when enough time has already elapsed."""

    last_at = str(_NOW - _MIN_SECONDS - 1.0)  # well past the threshold

    with patch("zahir.core.dependencies.rate_limit.time") as mock_time:
        mock_time.time.return_value = _NOW
        gen = _make_gen()
        effects, result = drain_to(gen, responses={EAcquire: True, EGetState: last_at, ESetState: None})

    sleep_effects = [eff for eff in effects if isinstance(eff, ESleep)]
    assert sleep_effects == [], "expected no ESleep when elapsed is already sufficient"
    assert result[0] == "satisfied"


def test_elapsed_too_short_emits_waiting_point_then_sleeps():
    """Proves the condition emits a WAITING bookman point then yields ESleep when elapsed < min_seconds."""

    last_at = str(_NOW - 0.1)  # only 0.1s ago, need 1.0s

    with patch("zahir.core.dependencies.rate_limit.time") as mock_time:
        mock_time.time.return_value = _NOW
        gen = _make_gen()
        next(gen)               # EAcquire
        gen.send(True)          # acquired → EGetState
        emit_effect = gen.send(last_at)   # elapsed=0.1s → EEmit(waiting point)
        sleep_effect = gen.send(None)     # emit done → ESleep

    assert isinstance(emit_effect, EEmit) and isinstance(emit_effect.body, Event), (
        "expected EEmit wrapping a bookman Event (waiting point) before ESleep"
    )
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
        with patch("zahir.core.dependencies.rate_limit.time") as mock_time:
            mock_time.time.return_value = _NOW
            gen = _make_gen(case["min_seconds"])
            next(gen)
            gen.send(True)
            gen.send(last_at)       # elapsed too short → EEmit waiting point
            sleep_effect = gen.send(None)  # emit done → ESleep

        assert isinstance(sleep_effect, ESleep), f"expected ESleep for elapsed={case['elapsed']}"
        assert abs(sleep_effect.ms - case["expected_ms"]) <= 10, (
            f"elapsed={case['elapsed']}s: expected ~{case['expected_ms']}ms sleep, got {sleep_effect.ms}ms"
        )


def test_satisfied_after_sleep_re_reads_state():
    """Proves that after ESleep the condition reads state again before checking elapsed."""

    last_at_initial = str(_NOW - 0.1)  # 0.1s ago → need to sleep 0.9s

    with patch("zahir.core.dependencies.rate_limit.time") as mock_time:
        mock_time.time.return_value = _NOW
        gen = _make_gen()
        next(gen)                       # EAcquire
        gen.send(True)                  # acquired → EGetState
        gen.send(last_at_initial)       # elapsed=0.1 → EEmit waiting point
        gen.send(None)                  # emit done → ESleep

        # Advance time so elapsed is now sufficient
        mock_time.time.return_value = _NOW + 1.0
        get_state_effect = gen.send(None)   # sleep done → re-read EGetState

    assert isinstance(get_state_effect, EGetState), "expected re-read of state after sleep"
