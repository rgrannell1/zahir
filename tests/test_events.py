"""Tests for event serialization and deserialization"""

from zahir.events import (
    JobCompletedEvent,
    JobIrrecoverableEvent,
    JobPrecheckFailedEvent,
    JobRecoveryStartedEvent,
    JobStartedEvent,
    JobTimeoutEvent,
    WorkflowCompleteEvent,
)
from zahir.exception import JobPrecheckError, exception_from_text_blob, exception_to_text_blob


def test_workflow_complete_event_roundtrip():
    """Test WorkflowCompleteEvent save/load roundtrip."""
    event = WorkflowCompleteEvent(workflow_id="wf-123", duration_seconds=42.5)

    saved = event.save()
    assert saved["workflow_id"] == "wf-123"
    assert saved["duration_seconds"] == 42.5

    loaded = WorkflowCompleteEvent.load(saved)
    assert loaded.workflow_id == "wf-123"
    assert loaded.duration_seconds == 42.5


def test_job_completed_event_roundtrip():
    """Test JobCompletedEvent save/load roundtrip."""

    event = JobCompletedEvent(workflow_id="wf-111", job_id="job-222", duration_seconds=15.3)

    saved = event.save()
    assert saved["workflow_id"] == "wf-111"
    assert saved["job_id"] == "job-222"
    assert saved["duration_seconds"] == 15.3

    loaded = JobCompletedEvent.load(saved)
    assert loaded.workflow_id == "wf-111"
    assert loaded.job_id == "job-222"
    assert loaded.duration_seconds == 15.3


def test_job_started_event_roundtrip():
    """Test JobStartedEvent save/load roundtrip."""

    event = JobStartedEvent(workflow_id="wf-333", job_id="job-444")

    saved = event.save()
    loaded = JobStartedEvent.load(saved)

    assert loaded.workflow_id == "wf-333"
    assert loaded.job_id == "job-444"


def test_job_timeout_event_roundtrip():
    """Test JobTimeoutEvent save/load roundtrip."""

    event = JobTimeoutEvent(workflow_id="wf-555", job_id="job-666", duration_seconds=30.0)

    saved = event.save()
    loaded = JobTimeoutEvent.load(saved)

    assert loaded.workflow_id == "wf-555"
    assert loaded.job_id == "job-666"
    assert loaded.duration_seconds == 30.0


def test_job_recovery_started_roundtrip():
    """Test JobRecoveryStarted save/load roundtrip."""

    event = JobRecoveryStartedEvent(workflow_id="wf-777", job_id="job-888")

    saved = event.save()
    loaded = JobRecoveryStartedEvent.load(saved)

    assert loaded.workflow_id == "wf-777"
    assert loaded.job_id == "job-888"


def test_job_irrecoverable_event_roundtrip():
    """Test JobIrrecoverableEvent save/load roundtrip."""
    error = ValueError("Something went wrong")

    event = JobIrrecoverableEvent(workflow_id="wf-ccc", error=error, job_id="job-ddd")

    saved = event.save()
    assert saved["workflow_id"] == "wf-ccc"
    assert saved["job_id"] == "job-ddd"
    assert saved["error"] == "Something went wrong"
    assert saved["error_type"] == "ValueError"

    loaded = JobIrrecoverableEvent.load(saved)
    assert loaded.workflow_id == "wf-ccc"
    assert loaded.job_id == "job-ddd"
    assert str(loaded.error) == "Something went wrong"


def test_job_precheck_failed_event_roundtrip():
    """Test JobPrecheckFailedEvent save/load roundtrip."""

    serialised_err = exception_to_text_blob(JobPrecheckError("failed"))
    event = JobPrecheckFailedEvent(workflow_id="wf-eee", job_id="job-fff", error=serialised_err)

    saved = event.save()

    loaded = JobPrecheckFailedEvent.load(saved)
    assert loaded.workflow_id == "wf-eee"
    assert loaded.job_id == "job-fff"
    assert exception_from_text_blob(loaded.error) == "failed"


def test_all_events_save_include_workflow_id():
    """Test that all events include workflow_id in their saved data."""

    events = [
        WorkflowCompleteEvent("wf-1", 1.0),
        JobCompletedEvent("wf-1", "job-1", 1.0),
        JobStartedEvent("wf-1", "job-1"),
        JobTimeoutEvent("wf-1", "job-1", 1.0),
        JobRecoveryStartedEvent("wf-1", "job-1"),
        JobIrrecoverableEvent("wf-1", Exception("err"), "job-1"),
        JobPrecheckFailedEvent("wf-1", "job-1", []),
    ]

    for event in events:
        saved = event.save()
        assert "workflow_id" in saved
        assert saved["workflow_id"] == "wf-1"
