"""Tests for the read_job_events function.

Tests the read_job_events function which processes job generator events,
handles awaits, outputs, and subjobs.
"""

import tempfile
import multiprocessing
import time

from zahir.base_types import Context, Job
from zahir.context import MemoryContext
from zahir.events import Await, JobEvent, JobOutputEvent, ZahirCustomEvent
from zahir.job_registry import SQLiteJobRegistry
from zahir.scope import LocalScope
from zahir.jobs.decorator import job
from zahir.worker.call_frame import ZahirStackFrame
from zahir.worker.state_machine import ZahirWorkerState
from zahir.worker.read_job_events import read_job_events


@job
def SimpleJobWithOutput(context: Context, input, dependencies):
    """A simple job that produces output."""
    yield JobOutputEvent({"result": "done"})


@job
def JobWithMultipleEvents(context: Context, input, dependencies):
    """A job that yields multiple events before output."""
    yield ZahirCustomEvent(output={"step": 1})
    yield ZahirCustomEvent(output={"step": 2})
    yield JobOutputEvent({"result": "complete"})


@job
def JobWithoutOutput(context: Context, input, dependencies):
    """A job that completes without output."""
    yield iter([])


@job
def AwaitingJob(context: Context, input, dependencies):
    """A job that awaits another job."""
    result = yield Await(SimpleJobWithOutput({"test": "data"}, {}))
    yield JobOutputEvent({"result": result})


@job
def SubjobCreator(context: Context, input, dependencies):
    """A job that creates subjobs."""
    subjob = SimpleJobWithOutput({"test": "data"}, {})
    yield subjob
    yield JobOutputEvent({"created": True})


def test_read_job_events_returns_output_event():
    """Test that read_job_events returns JobOutputEvent when job produces output."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-1")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJobWithOutput]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-1"

    worker_state = ZahirWorkerState(context, output_queue, workflow_id)

    # Add a job
    job = SimpleJobWithOutput({"test": "data"}, {})
    job_id = context.job_registry.add(job, output_queue)

    # Set up frame
    job_generator = SimpleJobWithOutput.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Read job events
    result = read_job_events(
        job_generator,
        job_registry=context.job_registry,
        output_queue=output_queue,
        state=worker_state,
        workflow_id=workflow_id,
        job_id=job_id,
    )

    # Should return JobOutputEvent
    assert isinstance(result, JobOutputEvent)
    assert result.output == {"result": "done"}


def test_read_job_events_returns_none_for_complete():
    """Test that read_job_events returns None when job completes without output."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-2")

    context = MemoryContext(scope=LocalScope(jobs=[JobWithoutOutput]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-2"

    worker_state = ZahirWorkerState(context, output_queue, workflow_id)

    # Add a job
    job = JobWithoutOutput({"test": "data"}, {})
    job_id = context.job_registry.add(job, output_queue)

    # Set up frame
    job_generator = JobWithoutOutput.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Read job events
    result = read_job_events(
        job_generator,
        job_registry=context.job_registry,
        output_queue=output_queue,
        state=worker_state,
        workflow_id=workflow_id,
        job_id=job_id,
    )

    # Should return None (completed)
    assert result is None


def test_read_job_events_returns_await():
    """Test that read_job_events returns Await when job awaits another job."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-3")

    context = MemoryContext(scope=LocalScope(jobs=[AwaitingJob, SimpleJobWithOutput]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-3"

    worker_state = ZahirWorkerState(context, output_queue, workflow_id)

    # Add a job
    job = AwaitingJob({"test": "data"}, {})
    job_id = context.job_registry.add(job, output_queue)

    # Set up frame
    job_generator = AwaitingJob.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Read job events
    result = read_job_events(
        job_generator,
        job_registry=context.job_registry,
        output_queue=output_queue,
        state=worker_state,
        workflow_id=workflow_id,
        job_id=job_id,
    )

    # Should return Await
    assert isinstance(result, Await)


def test_read_job_events_enqueues_intermediate_events():
    """Test that read_job_events enqueues intermediate events to output queue."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-4")

    context = MemoryContext(scope=LocalScope(jobs=[JobWithMultipleEvents]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-4"

    worker_state = ZahirWorkerState(context, output_queue, workflow_id)

    # Add a job
    job = JobWithMultipleEvents({"test": "data"}, {})
    job_id = context.job_registry.add(job, output_queue)

    # Clear the queue after job creation (job_registry.add puts JobEvent on queue)
    while not output_queue.empty():
        output_queue.get()

    # Set up frame
    job_generator = JobWithMultipleEvents.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Read job events
    result = read_job_events(
        job_generator,
        job_registry=context.job_registry,
        output_queue=output_queue,
        state=worker_state,
        workflow_id=workflow_id,
        job_id=job_id,
    )

    # Should return final output event
    assert isinstance(result, JobOutputEvent)

    # Give the queue background thread time to finish pickling
    time.sleep(0.1)

    # Collect all events from queue
    events = []
    while not output_queue.empty():
        events.append(output_queue.get(timeout=1))

    # Should have enqueued intermediate events and final output
    custom_events = [e for e in events if isinstance(e, ZahirCustomEvent)]
    output_events = [e for e in events if isinstance(e, JobOutputEvent)]

    assert len(custom_events) == 2
    assert custom_events[0].output == {"step": 1}
    assert custom_events[1].output == {"step": 2}
    assert len(output_events) == 1
    assert output_events[0].output == {"result": "complete"}


def test_read_job_events_sets_workflow_id():
    """Test that read_job_events sets workflow_id on events."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-5")

    context = MemoryContext(scope=LocalScope(jobs=[JobWithMultipleEvents]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "specific-workflow-123"

    worker_state = ZahirWorkerState(context, output_queue, workflow_id)

    # Add a job
    job = JobWithMultipleEvents({"test": "data"}, {})
    job_id = context.job_registry.add(job, output_queue)

    # Clear the queue after job creation (job_registry.add puts JobEvent on queue)
    while not output_queue.empty():
        output_queue.get()

    # Set up frame
    job_generator = JobWithMultipleEvents.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Read job events
    read_job_events(
        job_generator,
        job_registry=context.job_registry,
        output_queue=output_queue,
        state=worker_state,
        workflow_id=workflow_id,
        job_id=job_id,
    )

    # Give the queue background thread time to finish pickling
    time.sleep(0.1)

    # Check that custom events have workflow_id set
    events = []
    while not output_queue.empty():
        events.append(output_queue.get(timeout=1))

    custom_events = [e for e in events if isinstance(e, ZahirCustomEvent)]
    assert len(custom_events) > 0
    for event in custom_events:
        assert event.workflow_id == workflow_id


def test_read_job_events_sets_job_id():
    """Test that read_job_events sets job_id on events."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-6")

    context = MemoryContext(scope=LocalScope(jobs=[JobWithMultipleEvents]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-6"

    worker_state = ZahirWorkerState(context, output_queue, workflow_id)

    # Add a job
    job = JobWithMultipleEvents({"test": "data"}, {})
    job_id = context.job_registry.add(job, output_queue)

    # Clear the queue after job creation (job_registry.add puts JobEvent on queue)
    while not output_queue.empty():
        output_queue.get()

    # Set up frame
    job_generator = JobWithMultipleEvents.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Read job events
    read_job_events(
        job_generator,
        job_registry=context.job_registry,
        output_queue=output_queue,
        state=worker_state,
        workflow_id=workflow_id,
        job_id=job_id,
    )

    # Give the queue background thread time to finish pickling
    time.sleep(0.1)

    # Check that custom events have job_id set
    events = []
    while not output_queue.empty():
        events.append(output_queue.get(timeout=1))

    custom_events = [e for e in events if isinstance(e, ZahirCustomEvent)]
    assert len(custom_events) > 0
    for event in custom_events:
        assert event.job_id == job_id


def test_read_job_events_raises_on_non_iterator():
    """Test that read_job_events raises TypeError for non-iterator input."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-7")

    context = MemoryContext(scope=LocalScope(jobs=[SimpleJobWithOutput]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-7"

    worker_state = ZahirWorkerState(context, output_queue, workflow_id)

    # Add a job
    job = SimpleJobWithOutput({"test": "data"}, {})
    job_id = context.job_registry.add(job, output_queue)

    # Set up frame
    frame = ZahirStackFrame(job=job, job_generator=None, recovery=False)
    worker_state.frame = frame

    # Try to read events from non-iterator
    try:
        read_job_events(
            None,  # Not an iterator
            job_registry=context.job_registry,
            output_queue=output_queue,
            state=worker_state,
            workflow_id=workflow_id,
            job_id=job_id,
        )
        assert False, "Should have raised TypeError"
    except TypeError as err:
        assert "Non-iterator passed to read_job_events" in str(err)


def test_read_job_events_handles_subjobs():
    """Test that read_job_events adds subjobs to registry."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-8")

    context = MemoryContext(scope=LocalScope(jobs=[SubjobCreator, SimpleJobWithOutput]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-8"

    worker_state = ZahirWorkerState(context, output_queue, workflow_id)

    # Add a job
    job = SubjobCreator({"test": "data"}, {})
    job_id = context.job_registry.add(job, output_queue)

    # Give the queue background thread time to finish pickling
    time.sleep(0.1)

    # Clear the queue after job creation (job_registry.add puts JobEvent on queue)
    while not output_queue.empty():
        output_queue.get()

    # Set up frame
    job_generator = SubjobCreator.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Read job events
    result = read_job_events(
        job_generator,
        job_registry=context.job_registry,
        output_queue=output_queue,
        state=worker_state,
        workflow_id=workflow_id,
        job_id=job_id,
    )

    # Should return final output event
    assert isinstance(result, JobOutputEvent)
    assert result.output == {"created": True}

    # Give the queue background thread time to finish pickling
    time.sleep(0.1)

    # Collect events from queue
    events = []
    while not output_queue.empty():
        events.append(output_queue.get(timeout=1))

    # Should have JobEvent (from subjob) and JobOutputEvent
    job_events = [e for e in events if isinstance(e, JobEvent)]
    outputs = [e for e in events if isinstance(e, JobOutputEvent)]

    assert len(job_events) == 1, "Should have one subjob JobEvent in queue"
    assert len(outputs) == 1, "Should have final output in queue"


def test_read_job_events_sends_awaited_output():
    """Test that read_job_events sends awaited job output to generator."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-9")

    context = MemoryContext(scope=LocalScope(jobs=[AwaitingJob, SimpleJobWithOutput]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-9"

    worker_state = ZahirWorkerState(context, output_queue, workflow_id)

    # Add main job
    job = AwaitingJob({"test": "data"}, {})
    job_id = context.job_registry.add(job, output_queue)

    # Set up frame
    job_generator = AwaitingJob.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # First call returns Await
    result1 = read_job_events(
        job_generator,
        job_registry=context.job_registry,
        output_queue=output_queue,
        state=worker_state,
        workflow_id=workflow_id,
        job_id=job_id,
    )
    assert isinstance(result1, Await)

    # Simulate awaited job completing
    awaited_job = result1.job
    # Manually add the awaited job to registry (in real flow, handle_await does this)
    awaited_job_id = context.job_registry.add(awaited_job, output_queue)
    # Set output for the awaited job
    context.job_registry.set_output(awaited_job_id, workflow_id, output_queue, {"result": "done"})

    # Mark job as required
    worker_state.frame.required_jobs.add(awaited_job_id)

    # Second call should send output and return final output
    result2 = read_job_events(
        job_generator,
        job_registry=context.job_registry,
        output_queue=output_queue,
        state=worker_state,
        workflow_id=workflow_id,
        job_id=job_id,
    )

    # Should return final output
    assert isinstance(result2, JobOutputEvent)
    assert result2.output == {"result": {"result": "done"}}


def test_read_job_events_clears_required_jobs():
    """Test that read_job_events clears required_jobs after sending output."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-10")

    context = MemoryContext(scope=LocalScope(jobs=[AwaitingJob, SimpleJobWithOutput]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-10"

    worker_state = ZahirWorkerState(context, output_queue, workflow_id)

    # Add main job
    job = AwaitingJob({"test": "data"}, {})
    job_id = context.job_registry.add(job, output_queue)

    # Set up frame
    job_generator = AwaitingJob.run(context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Get await
    result1 = read_job_events(
        job_generator,
        job_registry=context.job_registry,
        output_queue=output_queue,
        state=worker_state,
        workflow_id=workflow_id,
        job_id=job_id,
    )

    # Simulate awaited job completing
    awaited_job = result1.job
    # Manually add the awaited job to registry (in real flow, handle_await does this)
    awaited_job_id = context.job_registry.add(awaited_job, output_queue)
    # Set output for the awaited job
    context.job_registry.set_output(awaited_job_id, workflow_id, output_queue, {"result": "done"})

    # Mark job as required
    worker_state.frame.required_jobs.add(awaited_job_id)
    assert len(worker_state.frame.required_jobs) == 1

    # Second call should clear required_jobs
    read_job_events(
        job_generator,
        job_registry=context.job_registry,
        output_queue=output_queue,
        state=worker_state,
        workflow_id=workflow_id,
        job_id=job_id,
    )

    # required_jobs should be cleared
    assert len(worker_state.frame.required_jobs) == 0
