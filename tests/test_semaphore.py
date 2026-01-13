"""Tests for Semaphore"""

import pytest

from zahir.base_types import DependencyState
from zahir.dependencies.semaphore import Semaphore


class MockContext:
    """Mock context for testing semaphores."""

    def __init__(self):
        self.state = {}


@pytest.fixture
def test_context():
    """Provide a test context with shared state."""
    return MockContext()


def test_semaphore_default_initial_state(test_context):
    """Test that semaphore defaults to SATISFIED state."""
    sem = Semaphore(context=test_context)
    assert sem.satisfied() == DependencyState.SATISFIED


def test_semaphore_custom_initial_state_satisfied(test_context):
    """Test that semaphore can be initialized to SATISFIED state."""
    sem = Semaphore(context=test_context, initial_state=DependencyState.SATISFIED)
    assert sem.satisfied() == DependencyState.SATISFIED


def test_semaphore_custom_initial_state_unsatisfied(test_context):
    """Test that semaphore can be initialized to UNSATISFIED state."""
    sem = Semaphore(context=test_context, initial_state=DependencyState.UNSATISFIED)
    assert sem.satisfied() == DependencyState.UNSATISFIED


def test_semaphore_custom_initial_state_impossible(test_context):
    """Test that semaphore can be initialized to IMPOSSIBLE state."""
    sem = Semaphore(context=test_context, initial_state=DependencyState.IMPOSSIBLE)
    assert sem.satisfied() == DependencyState.IMPOSSIBLE


def test_semaphore_open(test_context):
    """Test that open() sets the semaphore to SATISFIED."""
    sem = Semaphore(context=test_context, initial_state=DependencyState.UNSATISFIED)
    sem.open()
    assert sem.satisfied() == DependencyState.SATISFIED


def test_semaphore_close(test_context):
    """Test that close() sets the semaphore to UNSATISFIED."""
    sem = Semaphore(context=test_context, initial_state=DependencyState.SATISFIED)
    sem.close()
    assert sem.satisfied() == DependencyState.UNSATISFIED


def test_semaphore_abort(test_context):
    """Test that abort() sets the semaphore to IMPOSSIBLE."""
    sem = Semaphore(context=test_context, initial_state=DependencyState.SATISFIED)
    sem.abort()
    assert sem.satisfied() == DependencyState.IMPOSSIBLE


def test_semaphore_state_transitions(test_context):
    """Test that semaphore can transition through different states."""
    sem = Semaphore(context=test_context)

    # Start SATISFIED
    assert sem.satisfied() == DependencyState.SATISFIED

    # Close to UNSATISFIED
    sem.close()
    assert sem.satisfied() == DependencyState.UNSATISFIED

    # Reopen to SATISFIED
    sem.open()
    assert sem.satisfied() == DependencyState.SATISFIED

    # Abort to IMPOSSIBLE
    sem.abort()
    assert sem.satisfied() == DependencyState.IMPOSSIBLE

    # Can still transition from IMPOSSIBLE
    sem.open()
    assert sem.satisfied() == DependencyState.SATISFIED


def test_semaphore_multiple_close(test_context):
    """Test that calling close() multiple times keeps state UNSATISFIED."""
    sem = Semaphore(context=test_context)
    sem.close()
    sem.close()
    sem.close()
    assert sem.satisfied() == DependencyState.UNSATISFIED


def test_semaphore_multiple_open(test_context):
    """Test that calling open() multiple times keeps state SATISFIED."""
    sem = Semaphore(context=test_context, initial_state=DependencyState.UNSATISFIED)
    sem.open()
    sem.open()
    sem.open()
    assert sem.satisfied() == DependencyState.SATISFIED


def test_semaphore_multiple_abort(test_context):
    """Test that calling abort() multiple times keeps state IMPOSSIBLE."""
    sem = Semaphore(context=test_context)
    sem.abort()
    sem.abort()
    sem.abort()
    assert sem.satisfied() == DependencyState.IMPOSSIBLE


def test_semaphore_request_extension(test_context):
    """Test that request_extension returns self unchanged."""
    sem = Semaphore(context=test_context, initial_state=DependencyState.SATISFIED)
    result = sem.request_extension(100.0)

    # Should return self
    assert result is sem
    # State should be unchanged
    assert sem.satisfied() == DependencyState.SATISFIED


def test_semaphore_save_satisfied(test_context):
    """Test that save serializes a SATISFIED semaphore correctly."""
    sem = Semaphore(context=test_context, initial_state=DependencyState.SATISFIED)
    saved = sem.save(test_context)

    assert saved["type"] == "Semaphore"
    assert saved["state"] == "satisfied"


def test_semaphore_save_unsatisfied(test_context):
    """Test that save serializes an UNSATISFIED semaphore correctly."""
    sem = Semaphore(context=test_context, initial_state=DependencyState.UNSATISFIED)
    saved = sem.save(test_context)

    assert saved["type"] == "Semaphore"
    assert saved["state"] == "unsatisfied"


def test_semaphore_save_impossible(test_context):
    """Test that save serializes an IMPOSSIBLE semaphore correctly."""
    sem = Semaphore(context=test_context, initial_state=DependencyState.IMPOSSIBLE)
    saved = sem.save(test_context)

    assert saved["type"] == "Semaphore"
    assert saved["state"] == "impossible"


def test_semaphore_load_satisfied(test_context):
    """Test that load deserializes a SATISFIED semaphore correctly."""
    data = {"type": "Semaphore", "state": "satisfied"}

    sem = Semaphore.load(test_context, data)

    assert sem.satisfied() == DependencyState.SATISFIED


def test_semaphore_load_unsatisfied(test_context):
    """Test that load deserializes an UNSATISFIED semaphore correctly."""
    data = {"type": "Semaphore", "state": "unsatisfied"}

    sem = Semaphore.load(test_context, data)

    assert sem.satisfied() == DependencyState.UNSATISFIED


def test_semaphore_load_impossible(test_context):
    """Test that load deserializes an IMPOSSIBLE semaphore correctly."""
    data = {"type": "Semaphore", "state": "impossible"}

    sem = Semaphore.load(test_context, data)

    assert sem.satisfied() == DependencyState.IMPOSSIBLE


def test_semaphore_save_load_roundtrip(test_context):
    """Test that save/load preserves the semaphore state correctly."""
    # Test all three states
    for initial_state in [
        DependencyState.SATISFIED,
        DependencyState.UNSATISFIED,
        DependencyState.IMPOSSIBLE,
    ]:
        original = Semaphore(context=test_context, initial_state=initial_state)

        saved = original.save(test_context)
        loaded = Semaphore.load(test_context, saved)

        assert loaded.satisfied() == initial_state


def test_semaphore_save_after_state_change(test_context):
    """Test that save captures the current state after transitions."""
    sem = Semaphore(context=test_context, initial_state=DependencyState.SATISFIED)
    sem.close()

    saved = sem.save(test_context)
    assert saved["state"] == "unsatisfied"

    sem.open()
    saved = sem.save(test_context)
    assert saved["state"] == "satisfied"

    sem.abort()
    saved = sem.save(test_context)
    assert saved["state"] == "impossible"
