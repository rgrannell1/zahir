"""Tests for the check_preconditions state machine step.

Tests the check_preconditions function which validates job inputs before execution.
This is a critical gate that prevents invalid jobs from running.
"""

import multiprocessing
import tempfile

from zahir.base_types import Context, Job, JobState
from zahir.context import MemoryContext
from zahir.events import JobOutputEvent
from zahir.exception import JobPrecheckError
from zahir.job_registry import SQLiteJobRegistry
from zahir.jobs.decorator import job
from zahir.scope import LocalScope
from zahir.worker.call_frame import ZahirStackFrame
from zahir.worker.state_machine import ZahirWorkerState
from zahir.worker.state_machine.check_preconditions import check_preconditions
from zahir.worker.state_machine.states import (
    ExecuteJobStateChange,
    ExecuteRecoveryJobStateChange,
    HandleJobTimeoutStateChange,
    HandleRecoveryJobTimeoutStateChange,
    StartStateChange,
)


class ValidInputJob(Job):
    """A job with prechecks that always pass."""

    @staticmethod
    def precheck(input):
        return None

    @classmethod
    def run(cls, context: Context, input, dependencies):
        yield JobOutputEvent({"result": "success"})


class InvalidInputJob(Job):
    """A job with prechecks that always fail."""

    @staticmethod
    def precheck(input):
        return JobPrecheckError("input is invalid")

    @classmethod
    def run(cls, context: Context, input, dependencies):
        yield JobOutputEvent({"result": "should never execute"})


class MultipleErrorsJob(Job):
    """A job with prechecks that return multiple errors."""

    @staticmethod
    def precheck(input):
        errs = [
            JobPrecheckError("error one"),
            JobPrecheckError("error two"),
            JobPrecheckError("error three"),
        ]
        return ExceptionGroup("Precheck validation failed", errs)

    @classmethod
    def run(cls, context: Context, input, dependencies):
        yield JobOutputEvent({})


@job()
def TimeoutDuringPrecheckJob(context: Context, input, dependencies):
    """A job that times out during precheck validation."""

    yield JobOutputEvent({"result": "timeout"})


def test_check_preconditions_pass_normal_job():
    """Test that a job with valid input transitions to ExecuteJobStateChange."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-1")

    context = MemoryContext(scope=LocalScope(jobs=[ValidInputJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-1"

    # Initialize worker state
    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Create and register job
    job = ValidInputJob({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Create a frame for the job
    job_generator = ValidInputJob.run(context, job.input, job.dependencies)
    worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)

    # Run check_preconditions
    result, _ = check_preconditions(worker_state)

    # Verify result
    assert isinstance(result, ExecuteJobStateChange)
    assert result.data["message"] == "Prechecks passed; executing job"


def test_check_preconditions_pass_recovery_job():
    """Test that a recovery job with valid input transitions to ExecuteRecoveryJobStateChange."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-2")

    context = MemoryContext(scope=LocalScope(jobs=[ValidInputJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-2"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    job = ValidInputJob({"test": "data"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Create frame as recovery job
    job_generator = ValidInputJob.run(context, job.input, job.dependencies)
    worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)

    result, _ = check_preconditions(worker_state)

    assert isinstance(result, ExecuteRecoveryJobStateChange)
    assert result.data["message"] == "Prechecks passed; executing recovery job"


def test_check_preconditions_fail_single_error():
    """Test that a job with failing prechecks transitions to StartStateChange and updates job state."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-3")

    context = MemoryContext(scope=LocalScope(jobs=[InvalidInputJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-3"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    job = InvalidInputJob({"bad": "input"}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    job_generator = InvalidInputJob.run(context, job.input, job.dependencies)
    worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)

    result, _ = check_preconditions(worker_state)

    # Verify state transition
    assert isinstance(result, StartStateChange)
    assert result.data["message"] == "Prechecks failed; cleared job."

    # Verify frame was cleared
    assert worker_state.frame is None

    # Verify job state was updated in registry
    job_state = context.job_registry.get_state(job.job_id)
    assert job_state == JobState.PRECHECK_FAILED

    # Verify error was recorded
    errors = context.job_registry.get_errors(job.job_id)
    assert len(errors) == 1
    assert isinstance(errors[0], JobPrecheckError)
    assert "input is invalid" in str(errors[0])


def test_check_preconditions_fail_multiple_errors():
    """Test that multiple precheck errors are combined correctly."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-4")

    context = MemoryContext(scope=LocalScope(jobs=[MultipleErrorsJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-4"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    job = MultipleErrorsJob({}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    job_generator = MultipleErrorsJob.run(context, job.input, job.dependencies)
    worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)

    result, _ = check_preconditions(worker_state)

    assert isinstance(result, StartStateChange)

    # Verify all errors are included
    errors = context.job_registry.get_errors(job.job_id)
    assert len(errors) == 1
    err = errors[0]
    # The error should be an ExceptionGroup with all three errors
    assert isinstance(err, ExceptionGroup)
    # Check the nested exceptions directly
    assert len(err.exceptions) == 3
    assert any("error one" in str(exc) for exc in err.exceptions)
    assert any("error two" in str(exc) for exc in err.exceptions)
    assert any("error three" in str(exc) for exc in err.exceptions)


def test_check_preconditions_timeout_normal_job():
    """Test that a job that times out during prechecks transitions to HandleJobTimeoutStateChange."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-5")

    context = MemoryContext(scope=LocalScope(jobs=[TimeoutDuringPrecheckJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-5"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    # Create job with timeout
    job_instance = TimeoutDuringPrecheckJob({"test": "data"}, {})
    # Manually set job timeout to 0.001 seconds
    from zahir.base_types import JobOptions

    job_instance.job_options = JobOptions()
    job_instance.job_options.job_timeout = 0.001

    job_id = context.job_registry.add(context, job_instance, output_queue)

    # Set job to running state first and record start time
    context.job_registry.set_state(job_instance.job_id, workflow_id, output_queue, JobState.RUNNING)

    # Sleep long enough to ensure timeout
    import time

    time.sleep(0.01)

    job_generator = TimeoutDuringPrecheckJob.run(context, job_instance.input, job_instance.dependencies)
    worker_state.frame = ZahirStackFrame(job=job_instance, job_generator=job_generator, recovery=False)

    result, _ = check_preconditions(worker_state)

    # Verify we get timeout state change
    assert isinstance(result, HandleJobTimeoutStateChange)
    assert "timed out" in result.data["message"].lower()


def test_check_preconditions_timeout_recovery_job():
    """Test that a recovery job that times out during prechecks transitions to HandleRecoveryJobTimeoutStateChange."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-6")

    context = MemoryContext(scope=LocalScope(jobs=[TimeoutDuringPrecheckJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-6"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    job_instance = TimeoutDuringPrecheckJob({"test": "data"}, {})
    from zahir.base_types import JobOptions

    job_instance.job_options = JobOptions()
    job_instance.job_options.recover_timeout = 0.001

    job_id = context.job_registry.add(context, job_instance, output_queue)

    # Set job to recovering state
    context.job_registry.set_state(job_instance.job_id, workflow_id, output_queue, JobState.RECOVERING)

    import time

    time.sleep(0.01)

    job_generator = TimeoutDuringPrecheckJob.recover(context, job_instance.input, job_instance.dependencies, None)
    worker_state.frame = ZahirStackFrame(job=job_instance, job_generator=job_generator, recovery=True)

    result, _ = check_preconditions(worker_state)

    assert isinstance(result, HandleRecoveryJobTimeoutStateChange)
    assert "timed out" in result.data["message"].lower()


def test_check_preconditions_no_timeout_configured():
    """Test that jobs without timeout configuration proceed normally."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-7")

    context = MemoryContext(scope=LocalScope(jobs=[ValidInputJob]), job_registry=job_registry)
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-7"

    worker_state = ZahirWorkerState(context, None, output_queue, workflow_id)

    job = ValidInputJob({"test": "data"}, {})
    # Explicitly no job_options set
    job.job_options = None

    job_id = context.job_registry.add(context, job, output_queue)

    job_generator = ValidInputJob.run(context, job.input, job.dependencies)
    worker_state.frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)

    result, _ = check_preconditions(worker_state)

    # Should proceed to execute since no timeout is configured
    assert isinstance(result, ExecuteJobStateChange)
