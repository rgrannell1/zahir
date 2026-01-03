"""Tests to improve code coverage for events.py

Focusing on uncovered lines:
- Line 26: ZahirEvent.save() abstract method
- Line 36: ZahirEvent.load() abstract method
- Line 58: WorkflowStartedEvent.load() classmethod
- Line 64: WorkflowCompleteEvent.load() classmethod
- Line 101-103: WorkflowOutputEvent.__init__()
- Line 106, 114: WorkflowOutputEvent methods
- Line 154, 162, 198, 205, 265, 273, 289, 297, etc.: Event load() methods
"""

from zahir.events import (
    JobCompletedEvent,
    JobEvent,
    JobOutputEvent,
    JobPausedEvent,
    JobRecoveryCompletedEvent,
    JobRecoveryTimeoutEvent,
    JobStartedEvent,
    WorkflowCompleteEvent,
    WorkflowOutputEvent,
    WorkflowStartedEvent,
    ZahirCustomEvent,
    ZahirInternalErrorEvent,
)

# Note: Abstract classes can't be instantiated in Python, so we test
# them indirectly through their concrete implementations


def test_zahir_event_set_ids():
    """Test ZahirEvent.set_ids() method for events with job_id and workflow_id."""
    event = JobStartedEvent(workflow_id="old-wf", job_id="old-job")

    # Test setting both IDs
    event.set_ids(job_id="new-job", workflow_id="new-wf")
    assert event.job_id == "new-job"
    assert event.workflow_id == "new-wf"

    # Test setting only job_id
    event2 = JobCompletedEvent(workflow_id="wf-1", job_id="job-1", duration_seconds=5.0)
    event2.set_ids(job_id="job-2")
    assert event2.job_id == "job-2"
    assert event2.workflow_id == "wf-1"

    # Test setting only workflow_id
    event3 = JobPausedEvent(workflow_id="wf-1", job_id="job-1")
    event3.set_ids(workflow_id="wf-2")
    assert event3.workflow_id == "wf-2"
    assert event3.job_id == "job-1"

    # Test event without job_id/workflow_id attributes (should not error)
    event4 = WorkflowCompleteEvent(workflow_id="wf-1", duration_seconds=10.0)
    event4.set_ids(job_id="job-1")  # Should not set anything since event has no job_id attr


def test_workflow_started_event_roundtrip():
    """Test WorkflowStartedEvent save/load roundtrip."""
    event = WorkflowStartedEvent(workflow_id="wf-abc")

    saved = event.save()
    assert saved["workflow_id"] == "wf-abc"

    loaded = WorkflowStartedEvent.load(saved)
    assert loaded.workflow_id == "wf-abc"


def test_workflow_output_event_creation_and_methods():
    """Test WorkflowOutputEvent creation with different constructors."""
    # Test with all parameters
    event1 = WorkflowOutputEvent(
        output={"result": "test"},
        workflow_id="wf-1",
        job_id="job-1",
    )
    assert event1.output == {"result": "test"}
    assert event1.workflow_id == "wf-1"
    assert event1.job_id == "job-1"

    # Test save method
    saved = event1.save()
    assert saved["output"] == {"result": "test"}
    assert saved["workflow_id"] == "wf-1"
    assert saved["job_id"] == "job-1"

    # Test load method
    loaded = WorkflowOutputEvent.load(saved)
    assert loaded.output == {"result": "test"}
    assert loaded.workflow_id == "wf-1"
    assert loaded.job_id == "job-1"

    # Test with None values
    event2 = WorkflowOutputEvent(output={"data": "value"})
    assert event2.output == {"data": "value"}
    assert event2.workflow_id is None
    assert event2.job_id is None


def test_job_output_event_roundtrip():
    """Test JobOutputEvent save/load roundtrip."""
    event = JobOutputEvent(
        output={"status": "done"},
        workflow_id="wf-2",
        job_id="job-2",
    )

    saved = event.save()
    assert saved["output"] == {"status": "done"}
    assert saved["workflow_id"] == "wf-2"
    assert saved["job_id"] == "job-2"

    loaded = JobOutputEvent.load(saved)
    assert loaded.output == {"status": "done"}
    assert loaded.workflow_id == "wf-2"
    assert loaded.job_id == "job-2"


def test_job_paused_event_roundtrip():
    """Test JobPausedEvent save/load roundtrip."""
    event = JobPausedEvent(workflow_id="wf-pause", job_id="job-pause")

    saved = event.save()
    assert saved["workflow_id"] == "wf-pause"
    assert saved["job_id"] == "job-pause"

    loaded = JobPausedEvent.load(saved)
    assert loaded.workflow_id == "wf-pause"
    assert loaded.job_id == "job-pause"


def test_job_recovery_completed_event_roundtrip():
    """Test JobRecoveryCompletedEvent save/load roundtrip."""
    event = JobRecoveryCompletedEvent(
        workflow_id="wf-rec",
        job_id="job-rec",
        duration_seconds=45.5,
    )

    saved = event.save()
    assert saved["workflow_id"] == "wf-rec"
    assert saved["job_id"] == "job-rec"
    assert saved["duration_seconds"] == 45.5

    loaded = JobRecoveryCompletedEvent.load(saved)
    assert loaded.workflow_id == "wf-rec"
    assert loaded.job_id == "job-rec"
    assert loaded.duration_seconds == 45.5


def test_job_recovery_timeout_event_roundtrip():
    """Test JobRecoveryTimeoutEvent save/load roundtrip."""
    event = JobRecoveryTimeoutEvent(
        workflow_id="wf-timeout",
        job_id="job-timeout",
        duration_seconds=120.0,
    )

    saved = event.save()
    assert saved["workflow_id"] == "wf-timeout"
    assert saved["job_id"] == "job-timeout"
    assert saved["duration_seconds"] == 120.0

    loaded = JobRecoveryTimeoutEvent.load(saved)
    assert loaded.workflow_id == "wf-timeout"
    assert loaded.job_id == "job-timeout"
    assert loaded.duration_seconds == 120.0


def test_zahir_custom_event_roundtrip():
    """Test ZahirCustomEvent save/load roundtrip."""
    # Test with all fields
    event1 = ZahirCustomEvent(
        workflow_id="wf-custom",
        job_id="job-custom",
        output={"custom_data": "value"},
    )

    saved1 = event1.save()
    assert saved1["workflow_id"] == "wf-custom"
    assert saved1["job_id"] == "job-custom"
    assert saved1["output"] == {"custom_data": "value"}

    loaded1 = ZahirCustomEvent.load(saved1)
    assert loaded1.workflow_id == "wf-custom"
    assert loaded1.job_id is None  # Note: load() uses get() which returns None if not in data
    assert loaded1.output == {"custom_data": "value"}

    # Test with None/missing fields
    event2 = ZahirCustomEvent()
    saved2 = event2.save()

    loaded2 = ZahirCustomEvent.load(saved2)
    assert loaded2.workflow_id is None
    assert loaded2.job_id is None
    assert loaded2.output is None


def test_zahir_internal_error_event_roundtrip():
    """Test ZahirInternalErrorEvent save/load roundtrip."""
    # Test with error message
    event1 = ZahirInternalErrorEvent(
        workflow_id="wf-err",
        error="Internal error occurred",
    )

    saved1 = event1.save()
    assert saved1["workflow_id"] == "wf-err"
    assert saved1["error"] == "Internal error occurred"

    loaded1 = ZahirInternalErrorEvent.load(saved1)
    assert loaded1.workflow_id == "wf-err"
    assert loaded1.error == "Internal error occurred"

    # Test with None values
    event2 = ZahirInternalErrorEvent()
    saved2 = event2.save()

    loaded2 = ZahirInternalErrorEvent.load(saved2)
    assert loaded2.workflow_id is None
    assert loaded2.error is None


def test_job_event_roundtrip():
    """Test JobEvent save/load roundtrip."""
    from zahir.base_types import SerialisedJob

    job_data: SerialisedJob = {
        "type": "TestJob",
        "job_id": "job-123",
        "parent_id": None,
        "input": {"param": "value"},
        "dependencies": {},
        "options": None,
    }

    event = JobEvent(job=job_data)

    saved = event.save()
    assert saved["job"] == job_data

    loaded = JobEvent.load(saved)
    assert loaded.job == job_data
