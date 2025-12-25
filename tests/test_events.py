"""Tests for event serialization and deserialization"""

from unittest.mock import Mock
from zahir.events import (
    WorkflowCompleteEvent,
    JobRunnableEvent,
    JobCompletedEvent,
    JobStartedEvent,
    JobTimeoutEvent,
    JobRecoveryStarted,
    JobRecoveryCompleted,
    JobRecoveryTimeout,
    JobIrrecoverableEvent,
    JobPrecheckFailedEvent,
)


def test_workflow_complete_event_roundtrip():
    """Test WorkflowCompleteEvent save/load roundtrip."""
    event = WorkflowCompleteEvent(workflow_id="wf-123", duration_seconds=42.5)

    saved = event.save()
    assert saved["workflow_id"] == "wf-123"
    assert saved["duration_seconds"] == 42.5

    loaded = WorkflowCompleteEvent.load(saved)
    assert loaded.workflow_id == "wf-123"
    assert loaded.duration_seconds == 42.5


def test_job_runnable_event_roundtrip():
    """Test JobRunnableEvent save/load roundtrip."""
    mock_job = Mock()
    mock_job.__class__.__name__ = "TestJob"

    event = JobRunnableEvent(workflow_id="wf-456", job=mock_job, job_id="job-789")

    saved = event.save()
    assert saved["workflow_id"] == "wf-456"
    assert saved["job_id"] == "job-789"
    assert saved["job_type"] == "TestJob"

    loaded = JobRunnableEvent.load(saved)
    assert loaded.workflow_id == "wf-456"
    assert loaded.job_id == "job-789"
    assert loaded.job is None  # Jobs are not deserialized from events


def test_job_completed_event_roundtrip():
    """Test JobCompletedEvent save/load roundtrip."""
    mock_job = Mock()
    mock_job.__class__.__name__ = "CompletedJob"

    event = JobCompletedEvent(
        workflow_id="wf-111", job=mock_job, job_id="job-222", duration_seconds=15.3
    )

    saved = event.save()
    assert saved["workflow_id"] == "wf-111"
    assert saved["job_id"] == "job-222"
    assert saved["job_type"] == "CompletedJob"
    assert saved["duration_seconds"] == 15.3

    loaded = JobCompletedEvent.load(saved)
    assert loaded.workflow_id == "wf-111"
    assert loaded.job_id == "job-222"
    assert loaded.duration_seconds == 15.3
    assert loaded.job is None


def test_job_started_event_roundtrip():
    """Test JobStartedEvent save/load roundtrip."""
    mock_job = Mock()
    mock_job.__class__.__name__ = "StartedJob"

    event = JobStartedEvent(workflow_id="wf-333", job=mock_job, job_id="job-444")

    saved = event.save()
    loaded = JobStartedEvent.load(saved)

    assert loaded.workflow_id == "wf-333"
    assert loaded.job_id == "job-444"
    assert loaded.job is None


def test_job_timeout_event_roundtrip():
    """Test JobTimeoutEvent save/load roundtrip."""
    mock_job = Mock()
    mock_job.__class__.__name__ = "TimeoutJob"

    event = JobTimeoutEvent(
        workflow_id="wf-555", job=mock_job, job_id="job-666", duration_seconds=30.0
    )

    saved = event.save()
    loaded = JobTimeoutEvent.load(saved)

    assert loaded.workflow_id == "wf-555"
    assert loaded.job_id == "job-666"
    assert loaded.duration_seconds == 30.0
    assert loaded.job is None


def test_job_recovery_started_roundtrip():
    """Test JobRecoveryStarted save/load roundtrip."""
    mock_job = Mock()
    mock_job.__class__.__name__ = "RecoveryJob"

    event = JobRecoveryStarted(workflow_id="wf-777", job=mock_job, job_id="job-888")

    saved = event.save()
    loaded = JobRecoveryStarted.load(saved)

    assert loaded.workflow_id == "wf-777"
    assert loaded.job_id == "job-888"
    assert loaded.job is None


def test_job_recovery_completed_roundtrip():
    """Test JobRecoveryCompleted save/load roundtrip."""
    mock_job = Mock()
    mock_job.__class__.__name__ = "RecoveredJob"

    event = JobRecoveryCompleted(
        workflow_id="wf-999", job=mock_job, job_id="job-000", duration_seconds=5.7
    )

    saved = event.save()
    loaded = JobRecoveryCompleted.load(saved)

    assert loaded.workflow_id == "wf-999"
    assert loaded.job_id == "job-000"
    assert loaded.duration_seconds == 5.7
    assert loaded.job is None


def test_job_recovery_timeout_roundtrip():
    """Test JobRecoveryTimeout save/load roundtrip."""
    mock_job = Mock()
    mock_job.__class__.__name__ = "TimeoutRecoveryJob"

    event = JobRecoveryTimeout(workflow_id="wf-aaa", job=mock_job, job_id="job-bbb")

    saved = event.save()
    loaded = JobRecoveryTimeout.load(saved)

    assert loaded.workflow_id == "wf-aaa"
    assert loaded.job_id == "job-bbb"
    assert loaded.job is None


def test_job_irrecoverable_event_roundtrip():
    """Test JobIrrecoverableEvent save/load roundtrip."""
    mock_job = Mock()
    mock_job.__class__.__name__ = "IrrecoverableJob"
    error = ValueError("Something went wrong")

    event = JobIrrecoverableEvent(
        workflow_id="wf-ccc", error=error, job=mock_job, job_id="job-ddd"
    )

    saved = event.save()
    assert saved["workflow_id"] == "wf-ccc"
    assert saved["job_id"] == "job-ddd"
    assert saved["error"] == "Something went wrong"
    assert saved["error_type"] == "ValueError"

    loaded = JobIrrecoverableEvent.load(saved)
    assert loaded.workflow_id == "wf-ccc"
    assert loaded.job_id == "job-ddd"
    assert str(loaded.error) == "Something went wrong"
    assert loaded.job is None


def test_job_precheck_failed_event_roundtrip():
    """Test JobPrecheckFailedEvent save/load roundtrip."""
    mock_job = Mock()
    mock_job.__class__.__name__ = "PrecheckJob"

    event = JobPrecheckFailedEvent(
        workflow_id="wf-eee",
        job=mock_job,
        job_id="job-fff",
        errors=["Error 1", "Error 2", "Error 3"],
    )

    saved = event.save()
    assert saved["errors"] == ["Error 1", "Error 2", "Error 3"]

    loaded = JobPrecheckFailedEvent.load(saved)
    assert loaded.workflow_id == "wf-eee"
    assert loaded.job_id == "job-fff"
    assert loaded.errors == ["Error 1", "Error 2", "Error 3"]
    assert loaded.job is None


def test_all_events_save_include_workflow_id():
    """Test that all events include workflow_id in their saved data."""
    mock_job = Mock()
    mock_job.__class__.__name__ = "TestJob"

    events = [
        WorkflowCompleteEvent("wf-1", 1.0),
        JobRunnableEvent("wf-1", mock_job, "job-1"),
        JobCompletedEvent("wf-1", mock_job, "job-1", 1.0),
        JobStartedEvent("wf-1", mock_job, "job-1"),
        JobTimeoutEvent("wf-1", mock_job, "job-1", 1.0),
        JobRecoveryStarted("wf-1", mock_job, "job-1"),
        JobRecoveryCompleted("wf-1", mock_job, "job-1", 1.0),
        JobRecoveryTimeout("wf-1", mock_job, "job-1"),
        JobIrrecoverableEvent("wf-1", Exception("err"), mock_job, "job-1"),
        JobPrecheckFailedEvent("wf-1", mock_job, "job-1", []),
    ]

    for event in events:
        saved = event.save()
        assert "workflow_id" in saved
        assert saved["workflow_id"] == "wf-1"
