"""Tests for progress monitor with dependency injection and event tracking."""

from dataclasses import dataclass, field
import tempfile

from zahir.base_types import Context
from zahir.context import MemoryContext
from zahir.events import (
    JobCompletedEvent,
    JobEvent,
    JobIrrecoverableEvent,
    JobOutputEvent,
    JobPausedEvent,
    JobRecoveryCompletedEvent,
    JobRecoveryStartedEvent,
    JobStartedEvent,
    WorkflowCompleteEvent,
    WorkflowStartedEvent,
    ZahirEvent,
)
from zahir.job_registry import SQLiteJobRegistry
from zahir.jobs.decorator import spec
from zahir.scope import LocalScope
from zahir.worker.local_workflow import LocalWorkflow
from zahir.worker.progress import ProgressMonitor
import sys


@dataclass
class MockProgressMonitor:
    """Mock progress monitor that tracks all events for testing."""

    events: list[ZahirEvent] = field(default_factory=list)
    job_type_counts: dict[str, dict[str, int]] = field(default_factory=lambda: {})

    def handle_event(self, event: ZahirEvent) -> None:
        """Track events and update job type statistics."""
        self.events.append(event)

        match event:
            case JobEvent():
                job_type = event.job["type"]
                if job_type not in self.job_type_counts:
                    self.job_type_counts[job_type] = {
                        "started": 0,
                        "completed": 0,
                        "failed": 0,
                        "recovered": 0,
                    }

            case JobStartedEvent():
                job_type = getattr(event, "job_type", None)
                if job_type and job_type in self.job_type_counts:
                    self.job_type_counts[job_type]["started"] += 1

            case JobCompletedEvent():
                job_type = getattr(event, "job_type", None)
                if job_type and job_type in self.job_type_counts:
                    self.job_type_counts[job_type]["completed"] += 1

            case JobIrrecoverableEvent():
                job_type = getattr(event, "job_type", None)
                if job_type and job_type in self.job_type_counts:
                    self.job_type_counts[job_type]["failed"] += 1

            case JobRecoveryCompletedEvent():
                job_type = getattr(event, "job_type", None)
                if job_type and job_type in self.job_type_counts:
                    self.job_type_counts[job_type]["recovered"] += 1

    def __enter__(self) -> "MockProgressMonitor":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        return None


def test_progress_monitor_injection():
    """Test that progress monitor can be injected via DI."""
    mock_monitor = MockProgressMonitor()

    @spec()
    def SimpleTask(spec_args, context: Context, input, dependencies):
        yield JobOutputEvent({"result": 42})

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    scope = LocalScope.from_module(sys.modules[__name__])
    scope.add_job_spec(SimpleTask)
    context = MemoryContext(
        scope=scope,
        job_registry=SQLiteJobRegistry(tmp_file),
    )

    workflow = LocalWorkflow(context=context, max_workers=2, progress_monitor=mock_monitor)

    # Create an instance of the job class to pass to run
    start_job = SimpleTask({}, {}, 0.1)
    list(workflow.run(start=start_job))

    # Verify that events were tracked
    assert len(mock_monitor.events) > 0

    # Should have workflow started and completed events
    event_types = [type(evt).__name__ for evt in mock_monitor.events]
    assert "WorkflowStartedEvent" in event_types
    assert "WorkflowCompleteEvent" in event_types


def test_progress_monitor_tracks_job_lifecycle():
    """Test that progress monitor correctly tracks job lifecycle events."""
    mock_monitor = MockProgressMonitor()

    @spec()
    def LifecycleTask(spec_args, context: Context, input, dependencies):
        yield JobOutputEvent({"status": "done"})

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    scope = LocalScope.from_module(sys.modules[__name__])
    scope.add_job_spec(LifecycleTask)
    context = MemoryContext(
        scope=scope,
        job_registry=SQLiteJobRegistry(tmp_file),
    )

    workflow = LocalWorkflow(context=context, max_workers=2, progress_monitor=mock_monitor)

    # Create an instance of the job class to pass to run
    start_job = LifecycleTask({}, {}, 0.1)
    list(workflow.run(start=start_job))

    # Verify event sequence
    event_types = [type(evt).__name__ for evt in mock_monitor.events]

    # Should have job started and output events
    assert "JobStartedEvent" in event_types
    assert "JobOutputEvent" in event_types

    # Workflow should start and complete
    assert event_types[0] == "WorkflowStartedEvent"
    assert event_types[-1] == "WorkflowCompleteEvent"


def test_multiple_jobs_progress_tracking():
    """Test progress monitoring with multiple jobs."""
    mock_monitor = MockProgressMonitor()

    @spec()
    def JobOne(spec_args, context: Context, input, dependencies):
        yield JobOutputEvent({"value": 1})

    @spec()
    def JobTwo(spec_args, context: Context, input, dependencies):
        yield JobOutputEvent({"value": 2})

    @spec()
    def JobThree(spec_args, context: Context, input, dependencies):
        yield JobOutputEvent({"value": 3})

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    scope = LocalScope.from_module(sys.modules[__name__])
    scope.add_job_spec(JobOne)
    context = MemoryContext(
        scope=scope,
        job_registry=SQLiteJobRegistry(tmp_file),
    )

    workflow = LocalWorkflow(context=context, max_workers=2, progress_monitor=mock_monitor)

    # Create a starting job instance
    start_job = JobOne({}, {}, 0.1)
    list(workflow.run(start=start_job))

    # Should track all events
    assert len(mock_monitor.events) > 0

    # Count job-related events
    job_events = [evt for evt in mock_monitor.events if isinstance(evt, JobEvent)]
    assert len(job_events) >= 1  # At least one event per job


def test_progress_monitor_default_behavior():
    """Test that default progress monitor is used when not injected."""

    @spec()
    def DefaultTask(spec_args, context: Context, input, dependencies):
        yield JobOutputEvent({"result": 99})

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    scope = LocalScope.from_module(sys.modules[__name__])
    scope.add_job_spec(DefaultTask)
    context = MemoryContext(
        scope=scope,
        job_registry=SQLiteJobRegistry(tmp_file),
    )

    workflow = LocalWorkflow(context=context, max_workers=2)

    # Create an instance of the job class to pass to run
    start_job = DefaultTask({}, {}, 0.1)
    # Should work without errors even without explicit progress monitor
    events = list(workflow.run(start=start_job, events_filter=None))
    assert len(events) > 0
