"""Tests for MemoryJobRegistry"""

from unittest.mock import Mock
from zahir.registries.local import MemoryJobRegistry
from zahir.types import Job, JobState, DependencyState, Context
from zahir.dependencies.group import DependencyGroup
from typing import Iterator


class SimpleJob(Job):
    """Simple test job for registry tests."""

    @classmethod
    def run(
        cls, context: Context, input: dict, dependencies: DependencyGroup
    ) -> Iterator[Job | dict]:
        yield {"result": "done"}


def test_memory_job_registry_initialization():
    """Test that registry starts empty."""
    registry = MemoryJobRegistry()
    assert len(registry.jobs) == 0
    assert len(registry.outputs) == 0
    assert registry.pending() is False


def test_memory_job_registry_add_job():
    """Test adding a job to the registry."""
    registry = MemoryJobRegistry()
    job = SimpleJob(input={}, dependencies={})

    job_id = registry.add(job)

    assert job_id == job.job_id
    assert job_id in registry.jobs
    assert registry.jobs[job_id].job == job
    assert registry.jobs[job_id].state == JobState.PENDING


def test_memory_job_registry_add_multiple_jobs():
    """Test adding multiple jobs."""
    registry = MemoryJobRegistry()

    job1 = SimpleJob(input={"id": 1}, dependencies={})
    job2 = SimpleJob(input={"id": 2}, dependencies={})
    job3 = SimpleJob(input={"id": 3}, dependencies={})

    job_id1 = registry.add(job1)
    job_id2 = registry.add(job2)
    job_id3 = registry.add(job3)

    assert len(registry.jobs) == 3
    assert registry.jobs[job_id1].job == job1
    assert registry.jobs[job_id2].job == job2
    assert registry.jobs[job_id3].job == job3


def test_memory_job_registry_get_state():
    """Test getting job state."""
    registry = MemoryJobRegistry()
    job = SimpleJob(input={}, dependencies={})
    job_id = registry.add(job)

    state = registry.get_state(job_id)
    assert state == JobState.PENDING


def test_memory_job_registry_get_state_missing_job():
    """Test getting state for non-existent job raises KeyError."""
    registry = MemoryJobRegistry()

    try:
        registry.get_state("non-existent-job")
        assert False, "Should have raised KeyError"
    except KeyError as e:
        assert "not found in registry" in str(e)


def test_memory_job_registry_set_state():
    """Test setting job state."""
    registry = MemoryJobRegistry()
    job = SimpleJob(input={}, dependencies={})
    job_id = registry.add(job)

    registry.set_state(job_id, JobState.RUNNING)
    assert registry.get_state(job_id) == JobState.RUNNING

    registry.set_state(job_id, JobState.COMPLETED)
    assert registry.get_state(job_id) == JobState.COMPLETED


def test_memory_job_registry_set_output():
    """Test storing job output."""
    registry = MemoryJobRegistry()
    job = SimpleJob(input={}, dependencies={})
    job_id = registry.add(job)

    output = {"result": "success", "data": 42}
    registry.set_output(job_id, output)

    assert job_id in registry.outputs
    assert registry.outputs[job_id] == output


def test_memory_job_registry_get_output():
    """Test retrieving job output."""
    registry = MemoryJobRegistry()
    job = SimpleJob(input={}, dependencies={})
    job_id = registry.add(job)

    output = {"result": "success"}
    registry.set_output(job_id, output)

    retrieved = registry.get_output(job_id)
    assert retrieved == output


def test_memory_job_registry_get_output_none():
    """Test getting output for job without output."""
    registry = MemoryJobRegistry()
    job = SimpleJob(input={}, dependencies={})
    job_id = registry.add(job)

    output = registry.get_output(job_id)
    assert output is None


def test_memory_job_registry_pending_true():
    """Test pending returns True when jobs are pending."""
    registry = MemoryJobRegistry()
    job = SimpleJob(input={}, dependencies={})
    registry.add(job)

    assert registry.pending() is True


def test_memory_job_registry_pending_false():
    """Test pending returns False when no jobs are pending."""
    registry = MemoryJobRegistry()
    job = SimpleJob(input={}, dependencies={})
    job_id = registry.add(job)

    registry.set_state(job_id, JobState.COMPLETED)

    assert registry.pending() is False


def test_memory_job_registry_pending_mixed_states():
    """Test pending with jobs in different states."""
    registry = MemoryJobRegistry()

    job1 = SimpleJob(input={"id": 1}, dependencies={})
    job2 = SimpleJob(input={"id": 2}, dependencies={})
    job3 = SimpleJob(input={"id": 3}, dependencies={})

    job_id1 = registry.add(job1)
    job_id2 = registry.add(job2)
    registry.add(job3)

    registry.set_state(job_id1, JobState.COMPLETED)
    registry.set_state(job_id2, JobState.RUNNING)
    # job3 remains PENDING

    assert registry.pending() is True


def test_memory_job_registry_runnable():
    """Test getting runnable jobs."""
    registry = MemoryJobRegistry()

    # Create job with satisfied dependencies
    job = SimpleJob(input={}, dependencies={})
    job_id = registry.add(job)

    mock_context = Mock()
    runnable = list(registry.running(mock_context))

    assert len(runnable) == 1
    assert runnable[0][0] == job_id
    assert runnable[0][1] == job


def test_memory_job_registry_runnable_excludes_non_pending():
    """Test that runnable excludes non-pending jobs."""
    registry = MemoryJobRegistry()

    job1 = SimpleJob(input={"id": 1}, dependencies={})
    job2 = SimpleJob(input={"id": 2}, dependencies={})

    job_id1 = registry.add(job1)
    registry.add(job2)

    # Set job1 to completed
    registry.set_state(job_id1, JobState.COMPLETED)

    mock_context = Mock()
    runnable = list(registry.running(mock_context))

    # Only job2 should be runnable
    assert len(runnable) == 1
    assert runnable[0][1] == job2


def test_memory_job_registry_runnable_marks_impossible():
    """Test that runnable marks impossible jobs."""
    registry = MemoryJobRegistry()

    # Create job with impossible dependencies
    job = SimpleJob(input={}, dependencies={})
    job.ready = Mock(return_value=DependencyState.IMPOSSIBLE)
    job_id = registry.add(job)

    mock_context = Mock()
    runnable = list(registry.running(mock_context))

    # Job should not be runnable
    assert len(runnable) == 0
    # Job should be marked as impossible
    assert registry.get_state(job_id) == JobState.IMPOSSIBLE


def test_memory_job_registry_runnable_skips_unsatisfied():
    """Test that runnable skips unsatisfied jobs."""
    registry = MemoryJobRegistry()

    # Create job with unsatisfied dependencies
    job = SimpleJob(input={}, dependencies={})
    job.ready = Mock(return_value=DependencyState.UNSATISFIED)
    registry.add(job)

    mock_context = Mock()
    runnable = list(registry.running(mock_context))

    # Job should not be runnable
    assert len(runnable) == 0
