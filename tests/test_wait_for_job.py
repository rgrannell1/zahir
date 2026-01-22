"""Tests for the wait_for_job state machine step.

Tests the wait_for_job function which waits for job assignments from the overseer
in the push-based dispatch model.
"""

import multiprocessing
import sys
import tempfile
import threading
import time

from zahir.base_types import Context
from zahir.context import MemoryContext
from zahir.events import JobAssignedEvent, JobOutputEvent
from zahir.job_registry import SQLiteJobRegistry
from zahir.jobs.decorator import spec
from zahir.scope import LocalScope
from zahir.serialise import serialise_event
from zahir.worker.state_machine import ZahirWorkerState
from zahir.worker.state_machine.states import PopJobStateChange, StartStateChange
from zahir.worker.state_machine.wait_for_job import WAIT_TIMEOUT_SECONDS, wait_for_job


@spec()
def SimpleTestJob(context: Context, input, dependencies):
    """A simple test job for testing wait_for_job."""
    yield JobOutputEvent({"result": "done"})


def test_wait_for_job_receives_assignment():
    """Test that wait_for_job receives job assignment from input queue."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-1")

    # Register the job spec
    scope = LocalScope.from_module(sys.modules[__name__])
    context = MemoryContext(scope=scope, job_registry=job_registry)

    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-1"

    # Create a test job and add to registry
    test_job = SimpleTestJob({}, {})
    job_registry.add(context, test_job, output_queue, workflow_id)

    worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Put a job assignment on the input queue
    input_queue.put(
        serialise_event(
            context, JobAssignedEvent(workflow_id=workflow_id, job_id=test_job.job_id, job_type=test_job.spec.type)
        )
    )

    # Run wait_for_job
    result, returned_state = wait_for_job(worker_state)

    # Verify result is a StartStateChange indicating job was received
    assert isinstance(result, StartStateChange)
    assert test_job.job_id in result.data["message"]

    # Verify job was pushed to stack
    assert not worker_state.job_stack.is_empty()


def test_wait_for_job_checks_runnable_stack():
    """Test that wait_for_job returns PopJobStateChange when stack has runnable jobs."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-2")

    scope = LocalScope.from_module(sys.modules[__name__])
    context = MemoryContext(scope=scope, job_registry=job_registry)

    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-2"

    # Create a job and add to registry as PAUSED
    test_job = SimpleTestJob({}, {})
    job_registry.add(context, test_job, output_queue, workflow_id)

    worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Push a frame to the stack
    from zahir.worker.call_frame import ZahirStackFrame

    job_generator = SimpleTestJob.run(context, None, {})
    frame = ZahirStackFrame(job=test_job, job_generator=job_generator, recovery=False)
    worker_state.job_stack.push(frame)

    # Run wait_for_job - should immediately find runnable job on stack
    result, returned_state = wait_for_job(worker_state)

    # Should return PopJobStateChange since there's a runnable job on stack
    assert isinstance(result, PopJobStateChange)


def test_wait_for_job_times_out_and_rechecks():
    """Test that wait_for_job times out and rechecks for runnable jobs."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-3")

    scope = LocalScope.from_module(sys.modules[__name__])
    context = MemoryContext(scope=scope, job_registry=job_registry)

    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-3"

    worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Measure time - with no job on queue, it should timeout
    # Send an assignment after a delay from another thread
    def delayed_put():
        time.sleep(0.3)
        test_job_delayed = SimpleTestJob({}, {})
        job_registry.add(context, test_job_delayed, output_queue, workflow_id)
        input_queue.put(
            serialise_event(
                context,
                JobAssignedEvent(
                    workflow_id=workflow_id, job_id=test_job_delayed.job_id, job_type=test_job_delayed.spec.type
                ),
            )
        )

    thread = threading.Thread(target=delayed_put)
    thread.start()

    start_time = time.time()
    result, _ = wait_for_job(worker_state)
    elapsed = time.time() - start_time

    thread.join()

    # Should have received the delayed job assignment
    assert isinstance(result, StartStateChange)
    assert elapsed >= 0.25, f"Expected to wait at least 0.25s, got {elapsed}s"


def test_wait_for_job_handles_missing_job():
    """Test that wait_for_job handles missing job gracefully."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-4")

    scope = LocalScope.from_module(sys.modules[__name__])
    context = MemoryContext(scope=scope, job_registry=job_registry)

    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-4"

    worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Put an assignment for a job that doesn't exist
    input_queue.put(
        serialise_event(context, JobAssignedEvent(workflow_id=workflow_id, job_id="nonexistent-job", job_type=""))
    )
    # Then put a valid one so we don't hang
    test_job = SimpleTestJob({}, {})
    job_registry.add(context, test_job, output_queue, workflow_id)
    input_queue.put(
        serialise_event(
            context, JobAssignedEvent(workflow_id=workflow_id, job_id=test_job.job_id, job_type=test_job.spec.type)
        )
    )

    # Should handle missing job and continue
    result, _ = wait_for_job(worker_state)

    # First result might be error, but eventually should get valid job
    # or restart message
    assert isinstance(result, StartStateChange)


def test_wait_for_job_preserves_state():
    """Test that wait_for_job returns the same state object."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-5")

    scope = LocalScope.from_module(sys.modules[__name__])
    context = MemoryContext(scope=scope, job_registry=job_registry)

    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-5"

    # Create and add job
    test_job = SimpleTestJob({}, {})
    job_registry.add(context, test_job, output_queue, workflow_id)

    worker_state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Put assignment on queue
    input_queue.put(
        serialise_event(
            context, JobAssignedEvent(workflow_id=workflow_id, job_id=test_job.job_id, job_type=test_job.spec.type)
        )
    )

    # Run wait_for_job
    result, returned_state = wait_for_job(worker_state)

    # Verify state is the same object
    assert returned_state is worker_state
