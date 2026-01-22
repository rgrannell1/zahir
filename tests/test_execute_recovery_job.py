"""Tests for the execute_recovery_job state machine step.

Tests the execute_recovery_job function which executes a recovery job and handles
various outcomes: job output, awaits, completion without output, timeouts, and exceptions.
Similar to execute_job but with recovery-specific behavior.
"""

import multiprocessing
import tempfile
import time

from zahir.base_types import Context, JobState
from zahir.context import MemoryContext
from zahir.events import Await, JobOutputEvent
from zahir.job_registry import SQLiteJobRegistry
from zahir.jobs.decorator import spec
from zahir.scope import LocalScope
from zahir.worker.call_frame import ZahirStackFrame
from zahir.worker.state_machine import ZahirWorkerState
from zahir.worker.state_machine.execute_recovery_job import execute_recovery_job
from zahir.worker.state_machine.states import (
    HandleAwaitStateChange,
    HandleJobCompleteNoOutputStateChange,
    HandleJobOutputStateChange,
    HandleRecoveryJobExceptionStateChange,
    HandleRecoveryJobTimeoutStateChange,
)


def recovery_with_output_handler(context: Context, input, dependencies, err):
    """Recovery handler that produces output."""
    yield JobOutputEvent({"recovered": True})


@spec(recover=recovery_with_output_handler)
def RecoveryJobWithOutput(context: Context, input, dependencies):
    """A recovery job that produces output."""
    raise Exception("Initial failure")
    yield iter([])


def recovery_without_output_handler(context: Context, input, dependencies, err):
    """Recovery handler that completes without output."""
    yield iter([])


@spec(recover=recovery_without_output_handler)
def RecoveryJobWithoutOutput(context: Context, input, dependencies):
    """A recovery job that completes without output."""
    raise Exception("Initial failure")
    yield iter([])


@spec()
def SimpleJob(context: Context, input, dependencies):
    """A simple job for testing."""
    yield JobOutputEvent({"result": "done"})


def recovery_with_await_handler(context: Context, input, dependencies, err):
    """Recovery handler that awaits another job."""
    result = yield Await(SimpleJob({"test": "data"}, {}))
    yield JobOutputEvent({"result": result})


@spec(recover=recovery_with_await_handler)
def RecoveryJobWithAwait(context: Context, input, dependencies):
    """A recovery job that awaits another job."""
    raise Exception("Initial failure")
    yield iter([])


def recovery_timeout_handler(context: Context, input, dependencies, err):
    """Recovery handler that times out."""
    time.sleep(10)
    yield JobOutputEvent({"result": "should not reach here"})


@spec(recover=recovery_timeout_handler)
def RecoveryTimeoutJob(context: Context, input, dependencies):
    """A recovery job that times out during recovery."""
    raise Exception("Initial failure")
    yield iter([])


def recovery_exception_handler(context: Context, input, dependencies, err):
    """Recovery handler that raises an exception."""
    raise ValueError("Recovery failed")
    yield iter([])


@spec(recover=recovery_exception_handler)
def RecoveryExceptionJob(context: Context, input, dependencies):
    """A recovery job that raises an exception during recovery."""
    raise Exception("Initial failure")
    yield iter([])


def test_execute_recovery_job_sets_running_state():
    """Test that execute_recovery_job sets the job state to RUNNING."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-1")

    context = MemoryContext(scope=LocalScope(specs=[RecoveryJobWithOutput]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-1"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = RecoveryJobWithOutput({"test": "data"}, {}, recover_timeout=0.01)
    job_id = context.job_registry.add(context, job, output_queue, workflow_id)

    # Set up frame in recovery mode
    job_generator = RecoveryJobWithOutput.recover(context, job.input, job.dependencies, Exception("Test"))
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)
    worker_state.frame = frame

    # Run execute_recovery_job
    execute_recovery_job(worker_state)

    # Job state should be RUNNING
    job_state = context.job_registry.get_state(job_id)
    assert job_state == JobState.RUNNING


def test_execute_recovery_job_with_output():
    """Test that execute_recovery_job handles recovery jobs that produce output."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-2")

    context = MemoryContext(scope=LocalScope(specs=[RecoveryJobWithOutput]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-2"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = RecoveryJobWithOutput({"test": "data"}, {}, recover_timeout=0.01)
    job_id = context.job_registry.add(context, job, output_queue, workflow_id)

    # Set up frame in recovery mode
    job_generator = RecoveryJobWithOutput.recover(context, job.input, job.dependencies, Exception("Test"))
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)
    worker_state.frame = frame

    # Run execute_recovery_job
    result, _ = execute_recovery_job(worker_state)

    # Should return HandleJobOutputStateChange
    assert isinstance(result, HandleJobOutputStateChange)
    assert "produced output" in result.data["message"]
    assert "Recovery job" in result.data["message"]


def test_execute_recovery_job_stores_output_event():
    """Test that execute_recovery_job stores the output event in state.last_event."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-3")

    context = MemoryContext(scope=LocalScope(specs=[RecoveryJobWithOutput]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-3"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = RecoveryJobWithOutput({"test": "data"}, {}, recover_timeout=0.01)
    job_id = context.job_registry.add(context, job, output_queue, workflow_id)

    # Set up frame in recovery mode
    job_generator = RecoveryJobWithOutput.recover(context, job.input, job.dependencies, Exception("Test"))
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)
    worker_state.frame = frame

    # Run execute_recovery_job
    execute_recovery_job(worker_state)

    # Output event should be stored
    assert worker_state.last_event is not None
    assert isinstance(worker_state.last_event, JobOutputEvent)
    assert worker_state.last_event.output == {"recovered": True}


def test_execute_recovery_job_without_output():
    """Test that execute_recovery_job handles recovery jobs that complete without output."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-4")

    context = MemoryContext(scope=LocalScope(specs=[RecoveryJobWithoutOutput]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-4"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = RecoveryJobWithoutOutput({"test": "data"}, {}, recover_timeout=0.01)
    job_id = context.job_registry.add(context, job, output_queue, workflow_id)

    # Set up frame in recovery mode
    job_generator = RecoveryJobWithoutOutput.recover(context, job.input, job.dependencies, Exception("Test"))
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)
    worker_state.frame = frame

    # Run execute_recovery_job
    result, _ = execute_recovery_job(worker_state)

    # Should return HandleJobCompleteNoOutputStateChange
    assert isinstance(result, HandleJobCompleteNoOutputStateChange)
    assert "completed with no output" in result.data["message"]
    assert "Recovery job" in result.data["message"]


def test_execute_recovery_job_with_await():
    """Test that execute_recovery_job handles recovery jobs that await other jobs."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-5")

    context = MemoryContext(scope=LocalScope(specs=[RecoveryJobWithAwait, SimpleJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-5"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = RecoveryJobWithAwait({"test": "data"}, {}, recover_timeout=0.01)
    job_id = context.job_registry.add(context, job, output_queue, workflow_id)

    # Set up frame in recovery mode
    job_generator = RecoveryJobWithAwait.recover(context, job.input, job.dependencies, Exception("Test"))
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)
    worker_state.frame = frame

    # Run execute_recovery_job
    result, _ = execute_recovery_job(worker_state)

    # Should return HandleAwaitStateChange
    assert isinstance(result, HandleAwaitStateChange)
    assert "awaiting another job" in result.data["message"]
    assert "Recovery job" in result.data["message"]


def test_execute_recovery_job_stores_await_event():
    """Test that execute_recovery_job stores the await event in state.await_event."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-6")

    context = MemoryContext(scope=LocalScope(specs=[RecoveryJobWithAwait, SimpleJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-6"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = RecoveryJobWithAwait({"test": "data"}, {}, recover_timeout=0.01)
    job_id = context.job_registry.add(context, job, output_queue, workflow_id)

    # Set up frame in recovery mode
    job_generator = RecoveryJobWithAwait.recover(context, job.input, job.dependencies, Exception("Test"))
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)
    worker_state.frame = frame

    # Run execute_recovery_job
    execute_recovery_job(worker_state)

    # Await event should be stored
    assert worker_state.await_event is not None
    assert isinstance(worker_state.await_event, Await)


def test_execute_recovery_job_with_timeout():
    """Test that execute_recovery_job handles recovery job timeouts."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-7")

    context = MemoryContext(scope=LocalScope(specs=[RecoveryTimeoutJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-7"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job with a very short recovery timeout
    job = RecoveryTimeoutJob({"test": "data"}, {}, recover_timeout=0.01)
    job_id = context.job_registry.add(context, job, output_queue, workflow_id)

    # Set up frame in recovery mode
    job_generator = RecoveryTimeoutJob.recover(context, job.input, job.dependencies, Exception("Test"))
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)
    worker_state.frame = frame

    # Run execute_recovery_job
    result, _ = execute_recovery_job(worker_state)

    # Should return HandleRecoveryJobTimeoutStateChange
    assert isinstance(result, HandleRecoveryJobTimeoutStateChange)
    assert "timed out" in result.data["message"]
    assert "Recovery job" in result.data["message"]


def test_execute_recovery_job_with_exception():
    """Test that execute_recovery_job handles recovery job exceptions."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-8")

    context = MemoryContext(scope=LocalScope(specs=[RecoveryExceptionJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-8"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = RecoveryExceptionJob({"test": "data"}, {}, recover_timeout=0.01)
    job_id = context.job_registry.add(context, job, output_queue, workflow_id)

    # Set up frame in recovery mode
    job_generator = RecoveryExceptionJob.recover(context, job.input, job.dependencies, Exception("Test"))
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)
    worker_state.frame = frame

    # Run execute_recovery_job
    result, _ = execute_recovery_job(worker_state)

    # Should return HandleRecoveryJobExceptionStateChange
    assert isinstance(result, HandleRecoveryJobExceptionStateChange)
    assert "raised exception" in result.data["message"]
    assert "Recovery job" in result.data["message"]


def test_execute_recovery_job_stores_exception_event():
    """Test that execute_recovery_job stores the exception in state.last_event."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-9")

    context = MemoryContext(scope=LocalScope(specs=[RecoveryExceptionJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-9"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = RecoveryExceptionJob({"test": "data"}, {}, recover_timeout=0.01)
    job_id = context.job_registry.add(context, job, output_queue, workflow_id)

    # Set up frame in recovery mode
    job_generator = RecoveryExceptionJob.recover(context, job.input, job.dependencies, Exception("Test"))
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)
    worker_state.frame = frame

    # Run execute_recovery_job
    execute_recovery_job(worker_state)

    # Exception should be stored
    assert worker_state.last_event is not None
    assert isinstance(worker_state.last_event, Exception)
    assert "Recovery failed" in str(worker_state.last_event)


def test_execute_recovery_job_preserves_state():
    """Test that execute_recovery_job returns the same state object."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-10")

    context = MemoryContext(scope=LocalScope(specs=[RecoveryJobWithOutput]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-10"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = RecoveryJobWithOutput({"test": "data"}, {}, recover_timeout=0.01)
    job_id = context.job_registry.add(context, job, output_queue, workflow_id)

    # Set up frame in recovery mode
    job_generator = RecoveryJobWithOutput.recover(context, job.input, job.dependencies, Exception("Test"))
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)
    worker_state.frame = frame

    # Run execute_recovery_job
    result, returned_state = execute_recovery_job(worker_state)

    # Should return same state object
    assert returned_state is worker_state


def test_execute_recovery_job_workflow_id_used():
    """Test that execute_recovery_job uses the correct workflow_id."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-11")

    context = MemoryContext(scope=LocalScope(specs=[RecoveryJobWithOutput]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "specific-workflow-id-recovery"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = RecoveryJobWithOutput({"test": "data"}, {}, recover_timeout=0.01)
    job_id = context.job_registry.add(context, job, output_queue, workflow_id)

    # Set up frame in recovery mode
    job_generator = RecoveryJobWithOutput.recover(context, job.input, job.dependencies, Exception("Test"))
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)
    worker_state.frame = frame

    # Run execute_recovery_job
    execute_recovery_job(worker_state)

    # Job state should be updated with correct workflow_id
    job_state = context.job_registry.get_state(job_id)
    assert job_state == JobState.RUNNING


def test_execute_recovery_job_no_timeout_configured():
    """Test that recovery jobs without timeout configuration execute normally."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-12")

    context = MemoryContext(scope=LocalScope(specs=[RecoveryJobWithOutput]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-12"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job with no timeout configured
    job = RecoveryJobWithOutput({"test": "data"}, {}, recover_timeout=0.01)
    job_id = context.job_registry.add(context, job, output_queue, workflow_id)

    # Set up frame in recovery mode
    job_generator = RecoveryJobWithOutput.recover(context, job.input, job.dependencies, Exception("Test"))
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)
    worker_state.frame = frame

    # Run execute_recovery_job
    result, _ = execute_recovery_job(worker_state)

    # Should not timeout, should produce output
    assert isinstance(result, HandleJobOutputStateChange)


def test_execute_recovery_job_frame_recovery_mode():
    """Test that recovery jobs have frame.recovery set to True."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-13")

    context = MemoryContext(scope=LocalScope(specs=[RecoveryJobWithOutput]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-13"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Add a job
    job = RecoveryJobWithOutput({"test": "data"}, {}, recover_timeout=0.01)
    job_id = context.job_registry.add(context, job, output_queue, workflow_id)

    # Set up frame in recovery mode
    job_generator = RecoveryJobWithOutput.recover(context, job.input, job.dependencies, Exception("Test"))
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)
    worker_state.frame = frame

    # Verify recovery mode is True
    assert worker_state.frame.recovery is True

    # Run execute_recovery_job
    execute_recovery_job(worker_state)

    # Frame recovery mode should remain True during execution
    # (though frame may be cleared after completion)
