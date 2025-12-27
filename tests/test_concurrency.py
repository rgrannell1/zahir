"""Test ConcurrencyLimit dependency."""

from zahir.dependencies.concurrency import ConcurrencyLimit
from zahir.base_types import DependencyState


def test_concurrency_limit_claim():
    """Test that claiming slots increases the claimed count."""
    limit = ConcurrencyLimit(limit=3, slots=1)

    assert limit.claimed == 0

    limit.claim()
    assert limit.claimed == 1

    limit.claim()
    assert limit.claimed == 2


def test_concurrency_limit_free():
    """Test that freeing slots decreases the claimed count."""
    limit = ConcurrencyLimit(limit=3, slots=1)

    limit.claim()
    limit.claim()
    assert limit.claimed == 2

    limit.free()
    assert limit.claimed == 1

    limit.free()
    assert limit.claimed == 0


def test_concurrency_limit_free_minimum():
    """Test that freeing below zero keeps claimed at 0."""
    limit = ConcurrencyLimit(limit=3, slots=1)

    assert limit.claimed == 0
    limit.free()
    assert limit.claimed == 0


def test_concurrency_limit_satisfied():
    """Test that satisfied() returns correct state based on claims."""
    limit = ConcurrencyLimit(limit=3, slots=1)

    # No claims, should be satisfied
    assert limit.satisfied() == DependencyState.SATISFIED

    # Claim one slot, still satisfied (1 <= 3-1)
    limit.claim()
    assert limit.satisfied() == DependencyState.SATISFIED

    # Claim second slot, still satisfied (2 <= 3-1)
    limit.claim()
    assert limit.satisfied() == DependencyState.SATISFIED

    # Claim third slot, now unsatisfied (3 > 3-1)
    limit.claim()
    assert limit.satisfied() == DependencyState.UNSATISFIED

    # Free one slot, satisfied again
    limit.free()
    assert limit.satisfied() == DependencyState.SATISFIED


def test_concurrency_limit_with_multiple_slots():
    """Test concurrency limit with slots > 1."""
    limit = ConcurrencyLimit(limit=5, slots=2)

    # Should be satisfied when claimed <= (5-2) = 3
    assert limit.satisfied() == DependencyState.SATISFIED

    limit.claim()
    limit.claim()
    limit.claim()
    assert limit.satisfied() == DependencyState.SATISFIED

    # Fourth claim should make it unsatisfied (4 > 3)
    limit.claim()
    assert limit.satisfied() == DependencyState.UNSATISFIED


def test_concurrency_limit_context_manager():
    """Test using ConcurrencyLimit as a context manager."""
    limit = ConcurrencyLimit(limit=3, slots=1)

    assert limit.claimed == 0

    with limit:
        assert limit.claimed == 1

    assert limit.claimed == 0


def test_concurrency_limit_save():
    """Test that save() returns correct structure."""
    limit = ConcurrencyLimit(limit=5, slots=2)
    limit.claim()
    limit.claim()

    saved = limit.save()

    assert saved["type"] == "ConcurrencyLimit"
    assert saved["limit"] == 5
    assert saved["slots"] == 2
    assert saved["claimed"] == 0  # Reset on save


def test_concurrency_limit_load():
    """Test that load() reconstructs the limit correctly."""
    data = {"type": "ConcurrencyLimit", "limit": 5, "slots": 2, "claimed": 0}

    limit = ConcurrencyLimit.load(None, data)

    assert limit.limit == 5
    assert limit.slots == 2
    assert limit.claimed == 0


def test_concurrency_limit_save_load_roundtrip():
    """Test that save/load preserves limit configuration."""
    original = ConcurrencyLimit(limit=10, slots=3)
    original.claim()
    original.claim()

    # Save and load
    saved = original.save()
    restored = ConcurrencyLimit.load(None, saved)

    # Check limit and slots are preserved
    assert restored.limit == original.limit
    assert restored.slots == original.slots

    # Check claimed is reset to 0
    assert restored.claimed == 0

    # Check behavior is the same
    assert restored.satisfied() == DependencyState.SATISFIED

    # Should be able to claim up to (limit - slots) times
    for _ in range(7):  # 10 - 3 = 7
        restored.claim()
        assert restored.satisfied() == DependencyState.SATISFIED

    # One more should make it unsatisfied
    restored.claim()
    assert restored.satisfied() == DependencyState.UNSATISFIED
