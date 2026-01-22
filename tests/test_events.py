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


def test_workflow_complete_event_roundtrip(simple_context):
    """Test WorkflowCompleteEvent save/load roundtrip."""
    event = WorkflowCompleteEvent(workflow_id="wf-123", duration_seconds=42.5)

    saved = event.save(simple_context)
    assert saved["workflow_id"] == "wf-123"
    assert saved["duration_seconds"] == 42.5

    loaded = WorkflowCompleteEvent.load(simple_context, saved)
    assert loaded.workflow_id == "wf-123"
    assert loaded.duration_seconds == 42.5


def test_job_completed_event_roundtrip(simple_context):
    """Test JobCompletedEvent save/load roundtrip."""
    event = JobCompletedEvent(workflow_id="wf-111", job_id="job-222", job_type="TestJob", duration_seconds=15.3)

    saved = event.save(simple_context)
    assert saved["workflow_id"] == "wf-111"
    assert saved["job_id"] == "job-222"
    assert saved["job_type"] == "TestJob"
    assert saved["duration_seconds"] == 15.3

    loaded = JobCompletedEvent.load(simple_context, saved)
    assert loaded.workflow_id == "wf-111"
    assert loaded.job_id == "job-222"
    assert loaded.job_type == "TestJob"
    assert loaded.duration_seconds == 15.3


def test_job_started_event_roundtrip(simple_context):
    """Test JobStartedEvent save/load roundtrip."""
    event = JobStartedEvent(workflow_id="wf-333", job_id="job-444", job_type="TestJob")

    saved = event.save(simple_context)
    loaded = JobStartedEvent.load(simple_context, saved)

    assert loaded.workflow_id == "wf-333"
    assert loaded.job_id == "job-444"
    assert loaded.job_type == "TestJob"


def test_job_timeout_event_roundtrip(simple_context):
    """Test JobTimeoutEvent save/load roundtrip."""
    event = JobTimeoutEvent(workflow_id="wf-555", job_id="job-666", job_type="TestJob", duration_seconds=30.0)

    saved = event.save(simple_context)
    loaded = JobTimeoutEvent.load(simple_context, saved)

    assert loaded.workflow_id == "wf-555"
    assert loaded.job_id == "job-666"
    assert loaded.job_type == "TestJob"
    assert loaded.duration_seconds == 30.0


def test_job_recovery_started_roundtrip(simple_context):
    """Test JobRecoveryStarted save/load roundtrip."""
    event = JobRecoveryStartedEvent(workflow_id="wf-777", job_id="job-888", job_type="TestJob")

    saved = event.save(simple_context)
    loaded = JobRecoveryStartedEvent.load(simple_context, saved)

    assert loaded.workflow_id == "wf-777"
    assert loaded.job_id == "job-888"
    assert loaded.job_type == "TestJob"


def test_job_irrecoverable_event_roundtrip(simple_context):
    """Test JobIrrecoverableEvent save/load roundtrip."""
    error = ValueError("Something went wrong")
    serialised_err = exception_to_text_blob(error)

    event = JobIrrecoverableEvent(workflow_id="wf-ccc", error=serialised_err, job_id="job-ddd", job_type="TestJob")

    saved = event.save(simple_context)
    assert saved["workflow_id"] == "wf-ccc"
    assert saved["job_id"] == "job-ddd"
    assert saved["job_type"] == "TestJob"
    assert saved["error"] == serialised_err

    loaded = JobIrrecoverableEvent.load(simple_context, saved)
    assert loaded.workflow_id == "wf-ccc"
    assert loaded.job_id == "job-ddd"
    assert loaded.job_type == "TestJob"
    assert str(exception_from_text_blob(loaded.error)) == "Something went wrong"


def test_job_precheck_failed_event_roundtrip(simple_context):
    """Test JobPrecheckFailedEvent save/load roundtrip."""
    serialised_err = exception_to_text_blob(JobPrecheckError("failed"))
    event = JobPrecheckFailedEvent(workflow_id="wf-eee", job_id="job-fff", job_type="TestJob", error=serialised_err)

    saved = event.save(simple_context)

    loaded = JobPrecheckFailedEvent.load(simple_context, saved)
    assert loaded.workflow_id == "wf-eee"
    assert loaded.job_id == "job-fff"
    assert loaded.job_type == "TestJob"
    assert str(exception_from_text_blob(loaded.error)) == "failed"


def test_all_events_save_include_workflow_id(simple_context):
    """Test that all events include workflow_id in their saved data."""
    events = [
        WorkflowCompleteEvent("wf-1", 1.0),
        JobCompletedEvent("wf-1", "job-1", "TestJob", 1.0),
        JobStartedEvent("wf-1", "job-1", "TestJob"),
        JobTimeoutEvent("wf-1", "job-1", "TestJob", 1.0),
        JobRecoveryStartedEvent("wf-1", "job-1", "TestJob"),
        JobIrrecoverableEvent("wf-1", exception_to_text_blob(Exception("err")), "job-1", "TestJob"),
        JobPrecheckFailedEvent("wf-1", "job-1", "TestJob", "error"),
    ]

    for event in events:
        saved = event.save(simple_context)
        assert "workflow_id" in saved
        assert saved["workflow_id"] == "wf-1"
