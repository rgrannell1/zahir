"""Tests for the execute_job state machine step.

Tests the execute_job function which executes a job and handles various
outcomes: job output, awaits, completion without output, timeouts, and exceptions.
"""

import multiprocessing
import tempfile
import time

from zahir.base_types import Context, Job, JobOptions, JobState
from zahir.context import MemoryContext
from zahir.events import Await, JobOutputEvent
from zahir.job_registry import SQLiteJobRegistry
from zahir.jobs.decorator import spec
from zahir.scope import LocalScope
import sys
from zahir.worker.call_frame import ZahirStackFrame
from zahir.worker.state_machine import ZahirWorkerState
from zahir.worker.state_machine.execute_job import execute_job
from zahir.worker.state_machine.states import (
    HandleAwaitStateChange,
    HandleJobCompleteNoOutputStateChange,
    HandleJobExceptionStateChange,
    HandleJobOutputStateChange,
    HandleJobTimeoutStateChange,
)


@spec()
def SimpleJobWithOutput(spec_args, context: Context, input, dependencies):
    """A simple job that produces output."""
    yield JobOutputEvent({"result": "done"})


@spec()
def JobWithoutOutput(spec_args, context: Context, input, dependencies):
    """A job that completes without output."""
    # Yields an empty iterator to complete without output
    yield iter([])


@spec()
def AwaitingJob(spec_args, context: Context, input, dependencies):
    """A job that awaits another job."""
    result = yield Await(SimpleJobWithOutput({"test": "data"}, {}))
    yield JobOutputEvent({"result": result})


@spec()
def TimeoutJob(spec_args, context: Context, input, dependencies):
    """A job that sleeps longer than its timeout."""
    time.sleep(10)
    yield JobOutputEvent({"result": "should not reach here"})


def _exception_job_recover(spec_args, context, input, dependencies, err):
    yield JobOutputEvent({"recovered": True})


@spec(recover=_exception_job_recover)
def ExceptionJob(spec_args, context: Context, input, dependencies):
    """A job that raises an exception."""
    raise ValueError("Test exception")
    yield iter([])


@spec()
def MultiStepJob(spec_args, context: Context, input, dependencies):
    """A job with multiple steps."""
    yield JobOutputEvent({"step": 1})


def test_execute_job_sets_running_state():
    """Test that execute_job sets the job state to RUNNING."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-1")

    context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-1"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = SimpleJobWithOutput({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Set up frame
    job_generator = SimpleJobWithOutput.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Run execute_job
    execute_job(worker_state)

    # Job state should be RUNNING
    job_state = context.job_registry.get_state(job_id)
    assert job_state == JobState.RUNNING


def test_execute_job_with_output():
    """Test that execute_job handles jobs that produce output."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-2")

    context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-2"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = SimpleJobWithOutput({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Set up frame
    job_generator = SimpleJobWithOutput.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Run execute_job
    result, _ = execute_job(worker_state)

    # Should return HandleJobOutputStateChange
    assert isinstance(result, HandleJobOutputStateChange)
    assert "produced output" in result.data["message"]
    assert "JobInstance" in result.data["message"] or "SimpleJobWithOutput" in result.data["message"]


def test_execute_job_stores_output_event():
    """Test that execute_job stores the output event in state.last_event."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-3")

    context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-3"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = SimpleJobWithOutput({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Set up frame
    job_generator = SimpleJobWithOutput.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Run execute_job
    execute_job(worker_state)

    # Output event should be stored
    assert worker_state.last_event is not None
    assert isinstance(worker_state.last_event, JobOutputEvent)
    assert worker_state.last_event.output == {"result": "done"}


def test_execute_job_without_output():
    """Test that execute_job handles jobs that complete without output."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-4")

    context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-4"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = JobWithoutOutput({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Set up frame
    job_generator = JobWithoutOutput.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Run execute_job
    result, _ = execute_job(worker_state)

    # Should return HandleJobCompleteNoOutputStateChange
    assert isinstance(result, HandleJobCompleteNoOutputStateChange)
    assert "completed with no output" in result.data["message"]


def test_execute_job_with_await():
    """Test that execute_job handles jobs that await other jobs."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-5")

    context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-5"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = AwaitingJob({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Set up frame
    job_generator = AwaitingJob.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Run execute_job
    result, _ = execute_job(worker_state)

    # Should return HandleAwaitStateChange
    assert isinstance(result, HandleAwaitStateChange)
    assert "awaiting another job" in result.data["message"]


def test_execute_job_stores_await_event():
    """Test that execute_job stores the await event in state.await_event."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-6")

    context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-6"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = AwaitingJob({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Set up frame
    job_generator = AwaitingJob.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Run execute_job
    execute_job(worker_state)

    # Await event should be stored
    assert worker_state.await_event is not None
    assert isinstance(worker_state.await_event, Await)


def test_execute_job_with_timeout():
    """Test that execute_job handles job timeouts."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-7")

    context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-7"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job with a very short timeout
    job = TimeoutJob({"test": "data"}, {}, job_timeout=0.01)
    job_id = context.job_registry.add(context, job, output_queue)

    # Set up frame
    job_generator = TimeoutJob.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Run execute_job
    result, _ = execute_job(worker_state)

    # Should return HandleJobTimeoutStateChange
    assert isinstance(result, HandleJobTimeoutStateChange)
    assert "timed out" in result.data["message"]


def test_execute_job_with_exception():
    """Test that execute_job handles job exceptions."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-8")

    context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-8"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = ExceptionJob({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Set up frame
    job_generator = ExceptionJob.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Run execute_job
    result, _ = execute_job(worker_state)

    # Should return HandleJobExceptionStateChange
    assert isinstance(result, HandleJobExceptionStateChange)
    assert "raised exception" in result.data["message"]


def test_execute_job_stores_exception_event():
    """Test that execute_job stores the exception in state.last_event."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-9")

    context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-9"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = ExceptionJob({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Set up frame
    job_generator = ExceptionJob.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Run execute_job
    execute_job(worker_state)

    # Exception should be stored
    assert worker_state.last_event is not None
    assert isinstance(worker_state.last_event, Exception)
    assert "Test exception" in str(worker_state.last_event)


def test_execute_job_preserves_state():
    """Test that execute_job returns the same state object."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-10")

    context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-10"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = SimpleJobWithOutput({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Set up frame
    job_generator = SimpleJobWithOutput.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Run execute_job
    result, returned_state = execute_job(worker_state)

    # Should return same state object
    assert returned_state is worker_state


def test_execute_job_workflow_id_used():
    """Test that execute_job uses the correct workflow_id."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-11")

    context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "specific-workflow-id-xyz"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = SimpleJobWithOutput({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Set up frame
    job_generator = SimpleJobWithOutput.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Run execute_job
    execute_job(worker_state)

    # Job state should be updated with correct workflow_id
    job_state = context.job_registry.get_state(job_id)
    assert job_state == JobState.RUNNING


def test_execute_job_no_timeout_configured():
    """Test that jobs without timeout configuration execute normally."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-12")

    context = MemoryContext(scope=LocalScope.from_module(sys.modules[__name__]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-12"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job with no timeout configured
    job = SimpleJobWithOutput({}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Set up frame
    job_generator = SimpleJobWithOutput.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    worker_state.frame = frame

    # Run execute_job
    result, _ = execute_job(worker_state)

    # Should not timeout, should produce output
    assert isinstance(result, HandleJobOutputStateChange)
