"""Tests for Semaphore"""

from zahir.base_types import DependencyState
from zahir.dependencies.semaphore import Semaphore


def test_semaphore_default_initial_state():
    """Test that semaphore defaults to SATISFIED state."""
    sem = Semaphore()
    assert sem.satisfied() == DependencyState.SATISFIED


def test_semaphore_custom_initial_state_satisfied():
    """Test that semaphore can be initialized to SATISFIED state."""
    sem = Semaphore(initial_state=DependencyState.SATISFIED)
    assert sem.satisfied() == DependencyState.SATISFIED


def test_semaphore_custom_initial_state_unsatisfied():
    """Test that semaphore can be initialized to UNSATISFIED state."""
    sem = Semaphore(initial_state=DependencyState.UNSATISFIED)
    assert sem.satisfied() == DependencyState.UNSATISFIED


def test_semaphore_custom_initial_state_impossible():
    """Test that semaphore can be initialized to IMPOSSIBLE state."""
    sem = Semaphore(initial_state=DependencyState.IMPOSSIBLE)
    assert sem.satisfied() == DependencyState.IMPOSSIBLE


def test_semaphore_open():
    """Test that open() sets the semaphore to SATISFIED."""
    sem = Semaphore(initial_state=DependencyState.UNSATISFIED)
    sem.open()
    assert sem.satisfied() == DependencyState.SATISFIED


def test_semaphore_close():
    """Test that close() sets the semaphore to UNSATISFIED."""
    sem = Semaphore(initial_state=DependencyState.SATISFIED)
    sem.close()
    assert sem.satisfied() == DependencyState.UNSATISFIED


def test_semaphore_abort():
    """Test that abort() sets the semaphore to IMPOSSIBLE."""
    sem = Semaphore(initial_state=DependencyState.SATISFIED)
    sem.abort()
    assert sem.satisfied() == DependencyState.IMPOSSIBLE


def test_semaphore_state_transitions():
    """Test that semaphore can transition through different states."""
    sem = Semaphore()

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


def test_semaphore_multiple_close():
    """Test that calling close() multiple times keeps state UNSATISFIED."""
    sem = Semaphore()
    sem.close()
    sem.close()
    sem.close()
    assert sem.satisfied() == DependencyState.UNSATISFIED


def test_semaphore_multiple_open():
    """Test that calling open() multiple times keeps state SATISFIED."""
    sem = Semaphore(initial_state=DependencyState.UNSATISFIED)
    sem.open()
    sem.open()
    sem.open()
    assert sem.satisfied() == DependencyState.SATISFIED


def test_semaphore_multiple_abort():
    """Test that calling abort() multiple times keeps state IMPOSSIBLE."""
    sem = Semaphore()
    sem.abort()
    sem.abort()
    sem.abort()
    assert sem.satisfied() == DependencyState.IMPOSSIBLE


def test_semaphore_request_extension():
    """Test that request_extension returns self unchanged."""
    sem = Semaphore(initial_state=DependencyState.SATISFIED)
    result = sem.request_extension(100.0)

    # Should return self
    assert result is sem
    # State should be unchanged
    assert sem.satisfied() == DependencyState.SATISFIED


def test_semaphore_save_satisfied():
    """Test that save serializes a SATISFIED semaphore correctly."""
    sem = Semaphore(initial_state=DependencyState.SATISFIED)
    saved = sem.save()

    assert saved["type"] == "Semaphore"
    assert saved["state"] == "satisfied"


def test_semaphore_save_unsatisfied():
    """Test that save serializes an UNSATISFIED semaphore correctly."""
    sem = Semaphore(initial_state=DependencyState.UNSATISFIED)
    saved = sem.save()

    assert saved["type"] == "Semaphore"
    assert saved["state"] == "unsatisfied"


def test_semaphore_save_impossible():
    """Test that save serializes an IMPOSSIBLE semaphore correctly."""
    sem = Semaphore(initial_state=DependencyState.IMPOSSIBLE)
    saved = sem.save()

    assert saved["type"] == "Semaphore"
    assert saved["state"] == "impossible"


def test_semaphore_load_satisfied():
    """Test that load deserializes a SATISFIED semaphore correctly."""
    data = {"type": "Semaphore", "state": "satisfied"}

    sem = Semaphore.load(None, data)  # type: ignore[arg-type]

    assert sem.satisfied() == DependencyState.SATISFIED


def test_semaphore_load_unsatisfied():
    """Test that load deserializes an UNSATISFIED semaphore correctly."""
    data = {"type": "Semaphore", "state": "unsatisfied"}

    sem = Semaphore.load(None, data)  # type: ignore[arg-type]

    assert sem.satisfied() == DependencyState.UNSATISFIED


def test_semaphore_load_impossible():
    """Test that load deserializes an IMPOSSIBLE semaphore correctly."""
    data = {"type": "Semaphore", "state": "impossible"}

    sem = Semaphore.load(None, data)  # type: ignore[arg-type]

    assert sem.satisfied() == DependencyState.IMPOSSIBLE


def test_semaphore_save_load_roundtrip():
    """Test that save/load preserves the semaphore state correctly."""
    # Test all three states
    for initial_state in [
        DependencyState.SATISFIED,
        DependencyState.UNSATISFIED,
        DependencyState.IMPOSSIBLE,
    ]:
        original = Semaphore(initial_state=initial_state)
        saved = original.save()
        loaded = Semaphore.load(None, saved)  # type: ignore[arg-type]

        assert loaded.satisfied() == initial_state


def test_semaphore_save_after_state_change():
    """Test that save captures the current state after transitions."""
    sem = Semaphore(initial_state=DependencyState.SATISFIED)
    sem.close()

    saved = sem.save()
    assert saved["state"] == "unsatisfied"

    sem.open()
    saved = sem.save()
    assert saved["state"] == "satisfied"

    sem.abort()
    saved = sem.save()
    assert saved["state"] == "impossible"
