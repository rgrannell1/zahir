"""Tests for state event creation"""

from zahir.base_types import JobState
from zahir.events import (
    JobCompletedEvent,
    JobIrrecoverableEvent,
    JobPausedEvent,
    JobPrecheckFailedEvent,
    JobRecoveryCompletedEvent,
    JobRecoveryStartedEvent,
    JobRecoveryTimeoutEvent,
    JobStartedEvent,
    JobTimeoutEvent,
)
from zahir.job_registry.state_event import create_state_event


def test_create_state_event_pending():
    """Test that PENDING state returns None."""
    event = create_state_event(JobState.PENDING, "wf-1", "job-1")
    assert event is None


def test_create_state_event_blocked():
    """Test that BLOCKED state returns None."""
    event = create_state_event(JobState.BLOCKED, "wf-1", "job-1")
    assert event is None


def test_create_state_event_impossible():
    """Test that IMPOSSIBLE state returns None."""
    event = create_state_event(JobState.IMPOSSIBLE, "wf-1", "job-1")
    assert event is None


def test_create_state_event_ready():
    """Test that READY state returns None."""
    event = create_state_event(JobState.READY, "wf-1", "job-1")
    assert event is None


def test_create_state_event_paused():
    """Test that PAUSED state creates JobPausedEvent."""
    event = create_state_event(JobState.PAUSED, "wf-2", "job-2")
    assert isinstance(event, JobPausedEvent)


def test_create_state_event_precheck_failed():
    """Test that PRECHECK_FAILED state creates JobPrecheckFailedEvent."""
    err = ValueError("precheck failed")
    event = create_state_event(JobState.PRECHECK_FAILED, "wf-3", "job-3", error=err)
    assert isinstance(event, JobPrecheckFailedEvent)


def test_create_state_event_precheck_failed_no_error():
    """Test that PRECHECK_FAILED state with no error creates event with empty error."""
    event = create_state_event(JobState.PRECHECK_FAILED, "wf-4", "job-4")
    assert isinstance(event, JobPrecheckFailedEvent)


def test_create_state_event_running():
    """Test that RUNNING state creates JobStartedEvent."""
    event = create_state_event(JobState.RUNNING, "wf-5", "job-5")
    assert isinstance(event, JobStartedEvent)


def test_create_state_event_completed():
    """Test that COMPLETED state creates JobCompletedEvent."""
    event = create_state_event(JobState.COMPLETED, "wf-6", "job-6")
    assert isinstance(event, JobCompletedEvent)


def test_create_state_event_recovering():
    """Test that RECOVERING state creates JobRecoveryStartedEvent."""
    event = create_state_event(JobState.RECOVERING, "wf-7", "job-7")
    assert isinstance(event, JobRecoveryStartedEvent)


def test_create_state_event_recovered():
    """Test that RECOVERED state creates JobRecoveryCompletedEvent."""
    event = create_state_event(JobState.RECOVERED, "wf-8", "job-8")
    assert isinstance(event, JobRecoveryCompletedEvent)


def test_create_state_event_timed_out():
    """Test that TIMED_OUT state creates JobTimeoutEvent."""
    event = create_state_event(JobState.TIMED_OUT, "wf-9", "job-9")
    assert isinstance(event, JobTimeoutEvent)


def test_create_state_event_recovery_timed_out():
    """Test that RECOVERY_TIMED_OUT state creates JobRecoveryTimeoutEvent."""
    event = create_state_event(JobState.RECOVERY_TIMED_OUT, "wf-10", "job-10")
    assert isinstance(event, JobRecoveryTimeoutEvent)


def test_create_state_event_irrecoverable():
    """Test that IRRECOVERABLE state creates JobIrrecoverableEvent."""
    err = RuntimeError("job failed")
    event = create_state_event(JobState.IRRECOVERABLE, "wf-11", "job-11", error=err)
    assert isinstance(event, JobIrrecoverableEvent)
