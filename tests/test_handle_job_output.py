"""Tests for the handle_job_output state machine step.

Tests the handle_job_output function which handles job output events,
stores the output, and transitions to the Start state.
"""

import multiprocessing
import sys
import tempfile

from zahir.base_types import Context
from zahir.context import MemoryContext
from zahir.events import JobOutputEvent
from zahir.job_registry import SQLiteJobRegistry
from zahir.jobs.decorator import spec
from zahir.scope import LocalScope
from zahir.worker.call_frame import ZahirStackFrame
from zahir.worker.state_machine import ZahirWorkerState
from zahir.worker.state_machine.handle_job_output import handle_job_output
from zahir.worker.state_machine.states import StartStateChange


@spec()
def SimpleJob(spec_args, context: Context, input, dependencies):
    """A simple job for testing."""
    yield JobOutputEvent({"result": "done"})


@spec()
def JobWithData(spec_args, context: Context, input, dependencies):
    """A job that outputs data."""
    yield JobOutputEvent({"count": 42, "name": "test"})


def test_handle_job_output_stores_output():
    """Test that handle_job_output stores the output in the job registry."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-1")

    context = MemoryContext(scope=LocalScope.from_module(__name__), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-1"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = SimpleJob({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Set up frame
    job_generator = SimpleJob.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Set up output event
    output_data = {"result": "done"}
    worker_state.last_event = JobOutputEvent(output_data)

    # Run handle_job_output
    result, _ = handle_job_output(worker_state)

    # Verify output was stored
    stored_output = context.job_registry.get_output(job_id)
    assert stored_output == output_data


def test_handle_job_output_transitions_to_start():
    """Test that handle_job_output returns StartStateChange."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-2")

    context = MemoryContext(scope=LocalScope.from_module(__name__), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-2"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = SimpleJob({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Set up frame
    job_generator = SimpleJob.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Set up output event
    worker_state.last_event = JobOutputEvent({"result": "done"})

    # Run handle_job_output
    result, _ = handle_job_output(worker_state)

    # Should transition to Start state
    assert isinstance(result, StartStateChange)
    assert "Setting job output" in result.data["message"]


def test_handle_job_output_clears_frame():
    """Test that handle_job_output clears the current frame."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-3")

    context = MemoryContext(scope=LocalScope.from_module(__name__), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-3"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = SimpleJob({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Set up frame
    job_generator = SimpleJob.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Verify frame is set
    assert worker_state.frame is not None

    # Set up output event
    worker_state.last_event = JobOutputEvent({"result": "done"})

    # Run handle_job_output
    handle_job_output(worker_state)

    # Frame should be cleared
    assert worker_state.frame is None


def test_handle_job_output_handles_complex_data():
    """Test that handle_job_output correctly handles complex output data."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-4")

    context = MemoryContext(scope=LocalScope.from_module(__name__), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-4"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = JobWithData({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Set up frame
    job_generator = JobWithData.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Set up output event with complex data
    output_data = {"count": 42, "name": "test", "nested": {"key": "value"}}
    worker_state.last_event = JobOutputEvent(output_data)

    # Run handle_job_output
    handle_job_output(worker_state)

    # Verify complex output was stored correctly
    stored_output = context.job_registry.get_output(job_id)
    assert stored_output == output_data
    assert stored_output["count"] == 42
    assert stored_output["name"] == "test"
    assert stored_output["nested"]["key"] == "value"


def test_handle_job_output_preserves_state():
    """Test that handle_job_output returns the same state object."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-5")

    context = MemoryContext(scope=LocalScope.from_module(__name__), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-5"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = SimpleJob({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Set up frame
    job_generator = SimpleJob.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Set up output event
    worker_state.last_event = JobOutputEvent({"result": "done"})

    # Run handle_job_output
    result, returned_state = handle_job_output(worker_state)

    # Should return same state object
    assert returned_state is worker_state


def test_handle_job_output_with_empty_output():
    """Test that handle_job_output handles empty output correctly."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-6")

    context = MemoryContext(scope=LocalScope.from_module(__name__), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-6"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = SimpleJob({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Set up frame
    job_generator = SimpleJob.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Set up output event with empty data
    output_data = {}
    worker_state.last_event = JobOutputEvent(output_data)

    # Run handle_job_output
    result, _ = handle_job_output(worker_state)

    # Verify empty output was stored
    stored_output = context.job_registry.get_output(job_id)
    assert stored_output == output_data
    assert isinstance(result, StartStateChange)


def test_handle_job_output_with_list_output():
    """Test that handle_job_output handles list output correctly."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-7")

    context = MemoryContext(scope=LocalScope.from_module(__name__), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-7"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = SimpleJob({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Set up frame
    job_generator = SimpleJob.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Set up output event with list data
    output_data = [1, 2, 3, "test", {"nested": True}]
    worker_state.last_event = JobOutputEvent(output_data)

    # Run handle_job_output
    handle_job_output(worker_state)

    # Verify list output was stored correctly
    stored_output = context.job_registry.get_output(job_id)
    assert stored_output == output_data
    assert len(stored_output) == 5
    assert stored_output[3] == "test"


def test_handle_job_output_workflow_id_used():
    """Test that handle_job_output uses the correct workflow_id."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-8")

    context = MemoryContext(scope=LocalScope.from_module(__name__), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "specific-workflow-id-123"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = SimpleJob({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Set up frame
    job_generator = SimpleJob.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Set up output event
    worker_state.last_event = JobOutputEvent({"result": "done"})

    # Run handle_job_output
    handle_job_output(worker_state)

    # Verify output was stored with correct workflow_id
    stored_output = context.job_registry.get_output(job_id)
    assert stored_output == {"result": "done"}


def test_handle_job_output_multiple_calls():
    """Test that handle_job_output can be called for different jobs."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-9")

    context = MemoryContext(scope=LocalScope.from_module(__name__), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-9"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add first job
    job1 = SimpleJob({"test": "data1"}, {})
    job_id1 = context.job_registry.add(context, job1, output_queue)

    # Set up frame for first job
    job_generator1 = SimpleJob.run(None, context, job1.input, job1.dependencies)
    frame1 = ZahirStackFrame(job=job1, job_generator=job_generator1, recovery=False)
    worker_state.frame = frame1
    worker_state.last_event = JobOutputEvent({"result": "first"})

    # Run handle_job_output for first job
    handle_job_output(worker_state)

    # Add second job
    job2 = JobWithData({"test": "data2"}, {})
    job_id2 = context.job_registry.add(context, job2, output_queue)

    # Set up frame for second job
    job_generator2 = JobWithData.run(None, context, job2.input, job2.dependencies)
    frame2 = ZahirStackFrame(job=job2, job_generator=job_generator2, recovery=False)
    worker_state.frame = frame2
    worker_state.last_event = JobOutputEvent({"result": "second"})

    # Run handle_job_output for second job
    handle_job_output(worker_state)

    # Verify both outputs were stored correctly
    stored_output1 = context.job_registry.get_output(job_id1)
    stored_output2 = context.job_registry.get_output(job_id2)
    assert stored_output1 == {"result": "first"}
    assert stored_output2 == {"result": "second"}
