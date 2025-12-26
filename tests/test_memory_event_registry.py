"""Tests for MemoryEventRegistry"""

from unittest.mock import Mock
from zahir.event_registry.memory import MemoryEventRegistry
from zahir.events import (
    WorkflowCompleteEvent,
    JobCompletedEvent,
    JobStartedEvent,
)


def test_memory_event_registry_initialization():
    """Test that registry starts empty."""
    registry = MemoryEventRegistry()
    assert len(registry.events) == 0


def test_memory_event_registry_register_single_event():
    """Test registering a single event."""
    registry = MemoryEventRegistry()
    event = WorkflowCompleteEvent(workflow_id="wf-1", duration_seconds=10.0)

    registry.register(event)

    assert len(registry.events) == 1
    assert registry.events[0] == event


def test_memory_event_registry_register_multiple_events():
    """Test registering multiple events."""
    registry = MemoryEventRegistry()

    event1 = WorkflowCompleteEvent(workflow_id="wf-1", duration_seconds=10.0)
    event2 = WorkflowCompleteEvent(workflow_id="wf-2", duration_seconds=20.0)
    event3 = WorkflowCompleteEvent(workflow_id="wf-3", duration_seconds=30.0)

    registry.register(event1)
    registry.register(event2)
    registry.register(event3)

    assert len(registry.events) == 3
    assert registry.events[0] == event1
    assert registry.events[1] == event2
    assert registry.events[2] == event3


def test_memory_event_registry_preserves_order():
    """Test that events are stored in the order they were registered."""
    registry = MemoryEventRegistry()

    event1 = JobStartedEvent(workflow_id="wf-1", job_id="job-1")
    event2 = JobCompletedEvent(
        workflow_id="wf-1", job_id="job-1", duration_seconds=5.0
    )
    event3 = JobStartedEvent(workflow_id="wf-1", job_id="job-2")

    registry.register(event1)
    registry.register(event2)
    registry.register(event3)

    assert registry.events[0] == event1
    assert registry.events[1] == event2
    assert registry.events[2] == event3


def test_memory_event_registry_different_event_types():
    """Test registering different types of events."""
    registry = MemoryEventRegistry()

    event1 = WorkflowCompleteEvent(workflow_id="wf-1", duration_seconds=10.0)
    event2 = JobStartedEvent(workflow_id="wf-1", job_id="job-1")
    event3 = JobCompletedEvent(
        workflow_id="wf-1", job_id="job-1", duration_seconds=5.0
    )

    registry.register(event1)
    registry.register(event2)
    registry.register(event3)

    assert len(registry.events) == 3
    assert isinstance(registry.events[0], WorkflowCompleteEvent)
    assert isinstance(registry.events[1], JobStartedEvent)
    assert isinstance(registry.events[2], JobCompletedEvent)
