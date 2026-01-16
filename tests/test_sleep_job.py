"""Tests for the Sleep job.

Tests the Sleep job which pauses execution for a specified duration.
"""

import multiprocessing
import tempfile
import time

from zahir.base_types import JobState
from zahir.context import MemoryContext
from zahir.job_registry import SQLiteJobRegistry
from zahir.jobs.sleep import Sleep
from zahir.scope import LocalScope
from zahir.worker.call_frame import ZahirStackFrame
from zahir.worker.state_machine import ZahirWorkerState
from zahir.worker.state_machine.execute_job import execute_job
from zahir.worker.state_machine.states import HandleJobCompleteNoOutputStateChange


def test_sleep_job_precheck_valid_duration():
    """Test that Sleep job precheck accepts valid durations."""
    result = Sleep.precheck(None, {"duration_seconds": 1.5})
    assert result is None


def test_sleep_job_precheck_zero_duration():
    """Test that Sleep job precheck accepts zero duration."""
    result = Sleep.precheck(None, {"duration_seconds": 0})
    assert result is None


def test_sleep_job_precheck_invalid_duration_negative():
    """Test that Sleep job precheck rejects negative duration."""
    result = Sleep.precheck(None, {"duration_seconds": -1})
    assert isinstance(result, ValueError)
    assert "non-negative" in str(result)


def test_sleep_job_precheck_invalid_duration_type():
    """Test that Sleep job precheck rejects non-numeric duration."""
    result = Sleep.precheck(None, {"duration_seconds": "invalid"})
    assert isinstance(result, ValueError)


def test_sleep_job_precheck_missing_duration():
    """Test that Sleep job precheck accepts missing duration."""
    result = Sleep.precheck(None, {})
    assert result is None


def test_sleep_job_executes():
    """Test that Sleep job executes and sleeps for the specified duration."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-sleep")

    context = MemoryContext(scope=LocalScope(), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-sleep"

    # Test with a very short sleep to keep tests fast
    # Create JobInstance by calling the JobSpec
    job = Sleep({"duration_seconds": 0.1})
    job_id = context.job_registry.add(context, job, output_queue)

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)
    job_generator = Sleep.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    start_time = time.time()
    result, _ = execute_job(worker_state)
    elapsed_time = time.time() - start_time

    # Should have slept for at least 0.1 seconds
    assert elapsed_time >= 0.1

    # Should return HandleJobCompleteNoOutputStateChange since it returns output
    # Actually Sleep yields JobOutputEvent, so it should complete with output handled
    assert result is not None


def test_sleep_job_stores_output():
    """Test that Sleep job yields output event with the duration."""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-sleep-output")

    context = MemoryContext(scope=LocalScope(), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-sleep-output"

    duration = 0.05
    # Create JobInstance by calling the JobSpec
    job = Sleep({"duration_seconds": duration})
    job_id = context.job_registry.add(context, job, output_queue)

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)
    job_generator = Sleep.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    result, _ = execute_job(worker_state)

    # The last_event should contain the output event with the duration
    assert worker_state.last_event is not None
    assert hasattr(worker_state.last_event, "output")
    assert worker_state.last_event.output.get("duration_seconds") == duration
