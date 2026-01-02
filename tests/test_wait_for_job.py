"""Tests for the wait_for_job state machine step.

Tests the wait_for_job function which sleeps when no jobs are available.
This prevents busy-waiting when the job queue is empty.
"""

import tempfile
import time
import multiprocessing

from zahir.context import MemoryContext
from zahir.job_registry import SQLiteJobRegistry
from zahir.scope import LocalScope
from zahir.worker.state_machine import ZahirWorkerState
from zahir.worker.state_machine.wait_for_job import wait_for_job
from zahir.worker.state_machine.states import StartStateChange


def test_wait_for_job_returns_start_state_change():
    """Test that wait_for_job returns StartStateChange."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-1")

    context = MemoryContext(
        scope=LocalScope(jobs=[]),
        job_registry=job_registry
    )
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-1"

    worker_state = ZahirWorkerState(context, output_queue, workflow_id)

    # Run wait_for_job
    result, _ = wait_for_job(worker_state)

    # Verify result
    assert isinstance(result, StartStateChange)
    assert result.data["message"] == "Waited for job, restarting"


def test_wait_for_job_preserves_state():
    """Test that wait_for_job returns the same state object unchanged."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-2")

    context = MemoryContext(
        scope=LocalScope(jobs=[]),
        job_registry=job_registry
    )
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-2"

    worker_state = ZahirWorkerState(context, output_queue, workflow_id)

    # Run wait_for_job
    result, returned_state = wait_for_job(worker_state)

    # Verify state is unchanged and is the same object
    assert returned_state is worker_state


def test_wait_for_job_sleeps():
    """Test that wait_for_job actually sleeps for approximately 1 second."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-3")

    context = MemoryContext(
        scope=LocalScope(jobs=[]),
        job_registry=job_registry
    )
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-3"

    worker_state = ZahirWorkerState(context, output_queue, workflow_id)

    # Measure time
    start_time = time.time()
    wait_for_job(worker_state)
    elapsed = time.time() - start_time

    # Verify it slept for approximately 1 second (allow some tolerance)
    assert elapsed >= 0.95, f"Expected sleep >= 0.95s, got {elapsed}s"
    assert elapsed <= 1.1, f"Expected sleep <= 1.1s, got {elapsed}s"


def test_wait_for_job_with_empty_stack():
    """Test wait_for_job works correctly when job stack is empty."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-4")

    context = MemoryContext(
        scope=LocalScope(jobs=[]),
        job_registry=job_registry
    )
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-4"

    worker_state = ZahirWorkerState(context, output_queue, workflow_id)

    # Verify stack is empty
    assert worker_state.job_stack.is_empty()

    # Run wait_for_job
    result, _ = wait_for_job(worker_state)

    # Verify result
    assert isinstance(result, StartStateChange)
    # Stack should still be empty
    assert worker_state.job_stack.is_empty()


def test_wait_for_job_with_no_frame():
    """Test wait_for_job when there's no active frame."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-5")

    context = MemoryContext(
        scope=LocalScope(jobs=[]),
        job_registry=job_registry
    )
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-5"

    worker_state = ZahirWorkerState(context, output_queue, workflow_id)

    # Verify no active frame
    assert worker_state.frame is None

    # Run wait_for_job
    result, _ = wait_for_job(worker_state)

    # Verify result
    assert isinstance(result, StartStateChange)
    # Frame should still be None
    assert worker_state.frame is None


def test_wait_for_job_idempotent():
    """Test that calling wait_for_job multiple times behaves consistently."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-6")

    context = MemoryContext(
        scope=LocalScope(jobs=[]),
        job_registry=job_registry
    )
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-6"

    worker_state = ZahirWorkerState(context, output_queue, workflow_id)

    # Call wait_for_job multiple times
    result1, _ = wait_for_job(worker_state)
    result2, _ = wait_for_job(worker_state)
    result3, _ = wait_for_job(worker_state)

    # All should return the same type of result
    assert isinstance(result1, StartStateChange)
    assert isinstance(result2, StartStateChange)
    assert isinstance(result3, StartStateChange)

    # All should have the same message
    assert result1.data["message"] == result2.data["message"]
    assert result2.data["message"] == result3.data["message"]
