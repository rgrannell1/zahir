"""Tests for JobDependency"""

from unittest.mock import Mock

from zahir.base_types import DependencyState, JobState
from zahir.dependencies.job import JobDependency


def test_job_dependency_satisfied_when_completed():
    """Test that dependency is satisfied when job is in COMPLETED state."""
    mock_registry = Mock()
    mock_registry.get_state.return_value = JobState.COMPLETED

    dep = JobDependency("job-123", mock_registry)
    assert dep.satisfied() == DependencyState.SATISFIED
    mock_registry.get_state.assert_called_with("job-123")


def test_job_dependency_unsatisfied_when_pending():
    """Test that dependency is unsatisfied when job is PENDING."""
    mock_registry = Mock()
    mock_registry.get_state.return_value = JobState.PENDING

    dep = JobDependency("job-123", mock_registry)
    assert dep.satisfied() == DependencyState.UNSATISFIED


def test_job_dependency_unsatisfied_when_running():
    """Test that dependency is unsatisfied when job is RUNNING."""
    mock_registry = Mock()
    mock_registry.get_state.return_value = JobState.RUNNING

    dep = JobDependency("job-123", mock_registry)
    assert dep.satisfied() == DependencyState.UNSATISFIED


def test_job_dependency_impossible_when_irrecoverable():
    """Test that dependency is impossible when job is IRRECOVERABLE."""
    mock_registry = Mock()
    mock_registry.get_state.return_value = JobState.IRRECOVERABLE

    dep = JobDependency("job-123", mock_registry)
    assert dep.satisfied() == DependencyState.IMPOSSIBLE


def test_job_dependency_impossible_when_impossible_state():
    """Test that dependency is impossible when job is in IMPOSSIBLE state."""
    mock_registry = Mock()
    mock_registry.get_state.return_value = JobState.IMPOSSIBLE

    dep = JobDependency("job-123", mock_registry)
    assert dep.satisfied() == DependencyState.IMPOSSIBLE


def test_job_dependency_custom_satisfied_states():
    """Test that custom satisfied states work correctly."""
    mock_registry = Mock()
    mock_registry.get_state.return_value = JobState.RECOVERED

    # RECOVERED is not normally a satisfied state, but we make it one
    dep = JobDependency(
        "job-123",
        mock_registry,
        satisfied_states={JobState.COMPLETED, JobState.RECOVERED},
    )
    assert dep.satisfied() == DependencyState.SATISFIED


def test_job_dependency_custom_impossible_states():
    """Test that custom impossible states work correctly."""
    mock_registry = Mock()
    mock_registry.get_state.return_value = JobState.TIMED_OUT

    # Make TIMED_OUT an impossible state
    dep = JobDependency("job-123", mock_registry, impossible_states={JobState.TIMED_OUT})
    assert dep.satisfied() == DependencyState.IMPOSSIBLE


def test_job_dependency_save():
    """Test that save serializes correctly."""
    mock_registry = Mock()

    dep = JobDependency(
        "job-456",
        mock_registry,
        satisfied_states={JobState.COMPLETED, JobState.RECOVERED},
        impossible_states={JobState.IRRECOVERABLE},
    )

    saved = dep.save()

    assert saved["type"] == "JobDependency"
    assert saved["job_id"] == "job-456"
    assert set(saved["satisfied_states"]) == {"completed", "recovered"}
    assert saved["impossible_states"] == ["irrecoverable"]


def test_job_dependency_save_load_roundtrip():
    """Test that save/load preserves the dependency correctly."""
    mock_registry = Mock()

    dep = JobDependency(
        "job-789",
        mock_registry,
        satisfied_states={JobState.COMPLETED, JobState.RECOVERED},
        impossible_states={JobState.IRRECOVERABLE, JobState.TIMED_OUT},
    )

    saved = dep.save()

    # Create mock context
    mock_context = Mock()
    mock_context.job_registry = mock_registry

    loaded = JobDependency.load(mock_context, saved)

    assert loaded.job_id == "job-789"
    assert loaded.satisfied_states == {JobState.COMPLETED, JobState.RECOVERED}
    assert loaded.impossible_states == {JobState.IRRECOVERABLE, JobState.TIMED_OUT}
    assert loaded.job_registry == mock_registry


def test_job_dependency_save_load_with_defaults():
    """Test save/load with default states."""
    mock_registry = Mock()

    dep = JobDependency("job-default", mock_registry)
    saved = dep.save()

    mock_context = Mock()
    mock_context.job_registry = mock_registry

    loaded = JobDependency.load(mock_context, saved)

    # Should restore default satisfied and impossible states
    assert loaded.satisfied_states == {JobState.COMPLETED}
    assert loaded.impossible_states == {JobState.IRRECOVERABLE, JobState.IMPOSSIBLE}


def test_job_dependency_output():
    """Test retrieving job output."""
    mock_registry = Mock()
    mock_registry.get_output.return_value = {"result": "success", "data": 42}

    dep = JobDependency("job-output", mock_registry)

    mock_context = Mock()
    mock_context.job_registry = mock_registry

    output = dep.output(mock_context)

    assert output == {"result": "success", "data": 42}
    mock_registry.get_output.assert_called_with("job-output")


def test_job_dependency_output_none():
    """Test retrieving job output when no output is set."""
    mock_registry = Mock()
    mock_registry.get_output.return_value = None

    dep = JobDependency("job-no-output", mock_registry)

    mock_context = Mock()
    mock_context.job_registry = mock_registry

    output = dep.output(mock_context)

    assert output is None


def test_job_dependency_multiple_satisfied_states():
    """Test dependency with multiple satisfied states."""
    mock_registry = Mock()

    # Test that any of the satisfied states works
    dep = JobDependency(
        "job-multi",
        mock_registry,
        satisfied_states={JobState.COMPLETED, JobState.RECOVERED, JobState.TIMED_OUT},
    )

    # Test COMPLETED
    mock_registry.get_state.return_value = JobState.COMPLETED
    assert dep.satisfied() == DependencyState.SATISFIED

    # Test RECOVERED
    mock_registry.get_state.return_value = JobState.RECOVERED
    assert dep.satisfied() == DependencyState.SATISFIED

    # Test TIMED_OUT
    mock_registry.get_state.return_value = JobState.TIMED_OUT
    assert dep.satisfied() == DependencyState.SATISFIED

    # Test non-satisfied state
    mock_registry.get_state.return_value = JobState.PENDING
    assert dep.satisfied() == DependencyState.UNSATISFIED
