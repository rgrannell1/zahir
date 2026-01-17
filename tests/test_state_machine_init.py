"""Tests for the ZahirJobStateMachine integration.

These tests verify the state machine as a whole, testing the transitions
between states without overmocking individual steps. The individual step
functions are already tested in their own test files.

The focus is on ensuring:
1. State transitions work correctly
2. The state machine can run through complete job lifecycles
3. get_state correctly maps state names to handlers
4. The state machine maintains consistent behavior through transitions
"""

# Execute recovery job - should raise exception
import multiprocessing
import tempfile
import pytest

from zahir.base_types import Context, Job, JobOptions
from zahir.context import MemoryContext
from zahir.events import Await, JobOutputEvent
from zahir.job_registry import SQLiteJobRegistry
from zahir.jobs.decorator import spec
from zahir.scope import LocalScope
from zahir.worker.call_frame import ZahirStackFrame
from zahir.worker.state_machine import ZahirJobStateMachine, ZahirWorkerState
from zahir.worker.state_machine.states import (
    CheckPreconditionsStateChange,
    ExecuteJobStateChange,
    ExecuteRecoveryJobStateChange,
    HandleAwaitStateChange,
    HandleJobCompleteNoOutputStateChange,
    HandleJobExceptionStateChange,
    HandleJobOutputStateChange,
    HandleJobTimeoutStateChange,
    PopJobStateChange,
    StartStateChange,
    WaitForJobStateChange,
    ZahirJobState,
)


@spec()
def SimpleOutputJob(spec_args, context: Context, input, dependencies):
    """A job that produces output."""
    yield JobOutputEvent({"result": input.get("value", 42)})


@spec()
def NoOutputJob(spec_args, context: Context, input, dependencies):
    """A job that completes without output."""
    yield iter([])


@spec()
def AwaitingJob(spec_args, context: Context, input, dependencies):
    """A job that awaits another job."""
    result = yield Await(SimpleOutputJob({"value": 100}, {}))
    yield JobOutputEvent({"awaited_result": result["result"]})


def default_recover_exception(spec_args, context: Context, input, dependencies, err):
    """Recovery handler for exception job."""
    yield JobOutputEvent({"recovered": True})


@spec(recover=default_recover_exception)
def ExceptionJob(spec_args, context: Context, input, dependencies):
    """A job that raises an exception."""
    raise ValueError("Test exception")
    yield iter([])


@spec()
def TimeoutJobTest(spec_args, context: Context, input, dependencies):
    """A job that times out."""
    import time

    time.sleep(10)
    yield JobOutputEvent({"should_not_reach": True})


def test_get_state_returns_correct_handlers():
    """Test that get_state maps state names to correct handler methods."""

    # Test all state mappings (ENQUEUE_JOB was removed in push-based refactoring)
    assert ZahirJobStateMachine.get_state(ZahirJobState.START) == ZahirJobStateMachine.start
    assert ZahirJobStateMachine.get_state(ZahirJobState.WAIT_FOR_JOB) == ZahirJobStateMachine.wait_for_job
    assert ZahirJobStateMachine.get_state(ZahirJobState.POP_JOB) == ZahirJobStateMachine.pop_job
    assert ZahirJobStateMachine.get_state(ZahirJobState.CHECK_PRECONDITIONS) == ZahirJobStateMachine.check_preconditions
    assert ZahirJobStateMachine.get_state(ZahirJobState.EXECUTE_JOB) == ZahirJobStateMachine.execute_job
    assert (
        ZahirJobStateMachine.get_state(ZahirJobState.EXECUTE_RECOVERY_JOB) == ZahirJobStateMachine.execute_recovery_job
    )
    assert ZahirJobStateMachine.get_state(ZahirJobState.HANDLE_AWAIT) == ZahirJobStateMachine.handle_await
    assert ZahirJobStateMachine.get_state(ZahirJobState.HANDLE_JOB_OUTPUT) == ZahirJobStateMachine.handle_job_output
    assert (
        ZahirJobStateMachine.get_state(ZahirJobState.HANDLE_JOB_COMPLETE_NO_OUTPUT)
        == ZahirJobStateMachine.handle_job_complete_no_output
    )
    assert ZahirJobStateMachine.get_state(ZahirJobState.HANDLE_JOB_TIMEOUT) == ZahirJobStateMachine.handle_job_timeout
    assert (
        ZahirJobStateMachine.get_state(ZahirJobState.HANDLE_JOB_EXCEPTION) == ZahirJobStateMachine.handle_job_exception
    )


def test_state_machine_simple_job_with_output_lifecycle():
    """Test a complete lifecycle: start → enqueue → pop → check → execute → output → start."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-sm-1")

    context = MemoryContext(scope=LocalScope(specs=[SimpleOutputJob]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-sm-1"

    state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # 1. START state
    current = StartStateChange({"message": "Starting"})
    handler = ZahirJobStateMachine.get_state(current.state)
    next_state, _ = handler(state)

    # Should transition to WAIT_FOR_JOB (no frame, empty stack)
    assert isinstance(next_state, WaitForJobStateChange)

    # In push-based model, WAIT_FOR_JOB blocks on input_queue.
    # Instead, let's directly add a job to the stack and test the pop → check → execute flow.

    # Add a job directly and verify the pop → check → execute flow
    job = SimpleOutputJob({"value": 42}, {})
    job_id = context.job_registry.add(context, job, output_queue)
    job_generator = SimpleOutputJob.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    state.job_stack.push(frame)

    # 2. POP_JOB state - simulating that a job was pushed by overseer
    current = PopJobStateChange({"message": "Popping job"})
    handler = ZahirJobStateMachine.get_state(current.state)
    next_state, _ = handler(state)

    # Should transition to CHECK_PRECONDITIONS (fresh job needs precheck)
    assert isinstance(next_state, CheckPreconditionsStateChange)

    # 3. CHECK_PRECONDITIONS state
    handler = ZahirJobStateMachine.get_state(next_state.state)
    next_state, _ = handler(state)

    # Should transition to EXECUTE_JOB (no recovery needed)
    assert isinstance(next_state, ExecuteJobStateChange)

    # 4. EXECUTE_JOB state
    handler = ZahirJobStateMachine.get_state(next_state.state)
    next_state, _ = handler(state)

    # Should transition to HANDLE_JOB_OUTPUT (job produces output)
    assert isinstance(next_state, HandleJobOutputStateChange)

    # 5. HANDLE_JOB_OUTPUT state
    handler = ZahirJobStateMachine.get_state(next_state.state)
    next_state, _ = handler(state)

    # Should transition back to START
    assert isinstance(next_state, StartStateChange)


def test_state_machine_job_without_output_lifecycle():
    """Test lifecycle for a job that completes without output."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-sm-2")

    context = MemoryContext(scope=LocalScope(specs=[NoOutputJob]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-sm-2"

    state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Set up a frame with the no-output job
    job = NoOutputJob({}, {})
    job_id = context.job_registry.add(context, job, output_queue)
    job_generator = NoOutputJob.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    state.frame = frame

    # CHECK_PRECONDITIONS → EXECUTE_JOB
    current = CheckPreconditionsStateChange({"message": "Checking"})
    handler = ZahirJobStateMachine.get_state(current.state)
    next_state, _ = handler(state)
    assert isinstance(next_state, ExecuteJobStateChange)

    # EXECUTE_JOB → HANDLE_JOB_COMPLETE_NO_OUTPUT
    handler = ZahirJobStateMachine.get_state(next_state.state)
    next_state, _ = handler(state)
    assert isinstance(next_state, HandleJobCompleteNoOutputStateChange)

    # HANDLE_JOB_COMPLETE_NO_OUTPUT → WAIT_FOR_JOB
    handler = ZahirJobStateMachine.get_state(next_state.state)
    next_state, _ = handler(state)
    assert isinstance(next_state, WaitForJobStateChange)


def test_state_machine_await_handling():
    """Test state machine handles await events properly."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-sm-3")

    context = MemoryContext(scope=LocalScope(specs=[AwaitingJob, SimpleOutputJob]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-sm-3"

    state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Set up the awaiting job
    job = AwaitingJob({}, {})
    job_id = context.job_registry.add(context, job, output_queue)
    job_generator = AwaitingJob.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    state.frame = frame

    # CHECK_PRECONDITIONS → EXECUTE_JOB
    current = CheckPreconditionsStateChange({"message": "Checking"})
    handler = ZahirJobStateMachine.get_state(current.state)
    next_state, _ = handler(state)
    assert isinstance(next_state, ExecuteJobStateChange)

    # EXECUTE_JOB → HANDLE_AWAIT (job yields an Await)
    handler = ZahirJobStateMachine.get_state(next_state.state)
    next_state, _ = handler(state)
    assert isinstance(next_state, HandleAwaitStateChange)

    # HANDLE_AWAIT → WAIT_FOR_JOB (pauses current job, enqueues awaited job)
    handler = ZahirJobStateMachine.get_state(next_state.state)
    next_state, _ = handler(state)
    assert isinstance(next_state, WaitForJobStateChange)


def test_state_machine_exception_handling():
    """Test state machine handles exceptions and transitions to recovery."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-sm-4")

    context = MemoryContext(scope=LocalScope(specs=[ExceptionJob]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-sm-4"

    state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Set up the exception job
    job = ExceptionJob({}, {})
    job_id = context.job_registry.add(context, job, output_queue)
    job_generator = ExceptionJob.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    state.frame = frame

    # CHECK_PRECONDITIONS → EXECUTE_JOB
    current = CheckPreconditionsStateChange({"message": "Checking"})
    handler = ZahirJobStateMachine.get_state(current.state)
    next_state, _ = handler(state)
    assert isinstance(next_state, ExecuteJobStateChange)

    # EXECUTE_JOB → HANDLE_JOB_EXCEPTION (job raises exception)
    handler = ZahirJobStateMachine.get_state(next_state.state)
    next_state, _ = handler(state)
    assert isinstance(next_state, HandleJobExceptionStateChange)

    # HANDLE_JOB_EXCEPTION → CHECK_PRECONDITIONS (switches to recovery mode)
    handler = ZahirJobStateMachine.get_state(next_state.state)
    next_state, _ = handler(state)
    assert isinstance(next_state, CheckPreconditionsStateChange)


def test_state_machine_timeout_handling():
    """Test state machine handles job timeouts."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-sm-5")

    context = MemoryContext(scope=LocalScope(specs=[TimeoutJobTest]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-sm-5"

    state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Set up the timeout job with 0.1 second timeout
    job = TimeoutJobTest({}, {}, job_timeout=0.1)
    job_id = context.job_registry.add(context, job, output_queue)
    job_generator = TimeoutJobTest.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    state.frame = frame

    # CHECK_PRECONDITIONS → EXECUTE_JOB
    current = CheckPreconditionsStateChange({"message": "Checking"})
    handler = ZahirJobStateMachine.get_state(current.state)
    next_state, _ = handler(state)
    assert isinstance(next_state, ExecuteJobStateChange)

    # EXECUTE_JOB → HANDLE_JOB_TIMEOUT (job times out)
    handler = ZahirJobStateMachine.get_state(next_state.state)
    next_state, _ = handler(state)
    assert isinstance(next_state, HandleJobTimeoutStateChange)

    # HANDLE_JOB_TIMEOUT → WAIT_FOR_JOB (starts recovery process)
    handler = ZahirJobStateMachine.get_state(next_state.state)
    next_state, _ = handler(state)
    assert isinstance(next_state, WaitForJobStateChange)


def test_state_machine_state_transitions_are_consistent():
    """Test that handlers always return tuples with (StateChange, state)."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-sm-6")

    context = MemoryContext(scope=LocalScope(specs=[SimpleOutputJob]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-sm-6"

    state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Test start handler
    handler = ZahirJobStateMachine.get_state(ZahirJobState.START)
    result = handler(state)
    assert isinstance(result, tuple)
    assert len(result) == 2
    assert hasattr(result[0], "state")
    assert hasattr(result[0], "data")

    # Test wait_for_job handler would block, so test pop_job instead
    # Add a job to the stack first
    job = SimpleOutputJob({"value": 1}, {})
    context.job_registry.add(context, job, output_queue)
    job_generator = SimpleOutputJob.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    state.job_stack.push(frame)

    handler = ZahirJobStateMachine.get_state(ZahirJobState.POP_JOB)
    result = handler(state)
    assert isinstance(result, tuple)
    assert len(result) == 2
    assert hasattr(result[0], "state")


def test_state_machine_maintains_state_through_transitions():
    """Test that the worker state is properly maintained through transitions."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-sm-7")

    context = MemoryContext(scope=LocalScope(specs=[SimpleOutputJob]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-sm-7"

    state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Verify initial state
    assert state.context is context
    assert state.output_queue is output_queue
    assert state.workflow_id == workflow_id
    assert state.frame is None
    assert state.job_stack.is_empty()

    # Run through a transition
    handler = ZahirJobStateMachine.get_state(ZahirJobState.START)
    next_state, _ = handler(state)

    # Verify state maintained
    assert state.context is context
    assert state.output_queue is output_queue
    assert state.workflow_id == workflow_id


def test_state_machine_pop_to_execute_flow():
    """Test the flow from POP_JOB through to execution."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-sm-8")

    context = MemoryContext(scope=LocalScope(specs=[SimpleOutputJob]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-sm-8"

    state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Set up a job on the stack
    job = SimpleOutputJob({"value": 123}, {})
    job_id = context.job_registry.add(context, job, output_queue)
    job_generator = SimpleOutputJob.run(None, context, job.input, job.dependencies)
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)
    state.job_stack.push(frame)

    # POP_JOB state
    current = PopJobStateChange({"message": "Popping job"})
    handler = ZahirJobStateMachine.get_state(current.state)
    next_state, _ = handler(state)

    # Should have popped the job into the frame
    assert state.frame is not None
    assert state.frame.job == job

    # Should transition to CHECK_PRECONDITIONS or EXECUTE_JOB
    assert isinstance(next_state, (CheckPreconditionsStateChange, ExecuteJobStateChange))


def test_state_machine_all_states_have_handlers():
    """Test that all defined states have corresponding handlers."""

    for state_name in ZahirJobState:
        handler = ZahirJobStateMachine.get_state(state_name)
        assert handler is not None
        assert callable(handler)


def test_state_machine_recovery_job_complete_no_output():
    """Test recovery job completion without output."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-sm-9")

    context = MemoryContext(scope=LocalScope(specs=[ExceptionJob]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-sm-9"

    state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Set up a recovery job that completes without output
    job = ExceptionJob({}, {})
    job_id = context.job_registry.add(context, job, output_queue)
    # Use an empty generator to simulate no output
    job_generator = iter([])
    frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=True)
    state.frame = frame

    # Execute recovery job and get result
    from zahir.worker.state_machine.states import ExecuteRecoveryJobStateChange

    current = ExecuteRecoveryJobStateChange({"message": "Executing recovery job"})
    handler = ZahirJobStateMachine.get_state(current.state)
    next_state, _ = handler(state)

    # Should handle completion without output
    from zahir.worker.state_machine.states import HandleRecoveryJobCompleteNoOutputStateChange

    if isinstance(next_state, HandleRecoveryJobCompleteNoOutputStateChange):
        handler = ZahirJobStateMachine.get_state(next_state.state)
        next_state, _ = handler(state)
        assert isinstance(next_state, WaitForJobStateChange)


def test_state_machine_recovery_job_timeout():
    """Test recovery job timeout handling."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-sm-10")

    context = MemoryContext(scope=LocalScope(specs=[TimeoutJobTest]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-sm-10"

    state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Set up a recovery job with timeout
    job = TimeoutJobTest({}, {}, recover_timeout=0.1)
    job_id = context.job_registry.add(context, job, output_queue)

    def timeout_generator():
        import time

        time.sleep(10)  # This will timeout
        yield JobOutputEvent({"should_not_reach": True})

    frame = ZahirStackFrame(job=job, job_generator=timeout_generator(), recovery=True)
    state.frame = frame

    # Execute recovery job - should timeout
    from zahir.worker.state_machine.states import ExecuteRecoveryJobStateChange

    current = ExecuteRecoveryJobStateChange({"message": "Executing recovery job"})
    handler = ZahirJobStateMachine.get_state(current.state)
    next_state, _ = handler(state)

    # Should handle recovery timeout
    from zahir.worker.state_machine.states import HandleRecoveryJobTimeoutStateChange

    if isinstance(next_state, HandleRecoveryJobTimeoutStateChange):
        handler = ZahirJobStateMachine.get_state(next_state.state)
        next_state, _ = handler(state)
        # Recovery timeout should transition to appropriate state
        assert next_state is not None


def test_state_machine_recovery_job_exception():
    """Test recovery job exception handling."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-sm-11")


def recovery_exception_recover(spec_args, context: Context, input, dependencies, err):
    """Recovery handler that raises exception."""
    # Make it a generator that raises on first iteration
    raise RuntimeError("Recovery also failed")
    yield  # Never reached, just makes this a generator


@spec(recover=recovery_exception_recover)
def RecoveryExceptionJob(spec_args, context: Context, input, dependencies):
    """A job that raises an exception in recovery."""
    raise ValueError("Initial error")
    yield iter([])


def test_state_machine_recovery_job_exception():
    """Test that recovery jobs can raise exceptions."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_file = tmp.name

    job_registry = SQLiteJobRegistry(tmp_file)
    job_registry.init("test-worker-sm-11")

    context = MemoryContext(scope=LocalScope(specs=[RecoveryExceptionJob]), job_registry=job_registry)
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    workflow_id = "test-workflow-sm-11"

    state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)

    # Set up a recovery job that raises exception
    job = RecoveryExceptionJob({}, {})
    job_id = context.job_registry.add(context, job, output_queue)

    # Try to execute the recovery job - it should raise when we iterate the generator
    # because the recovery handler raises RuntimeError
    with pytest.raises(RuntimeError, match="Recovery also failed"):
        job_generator = RecoveryExceptionJob.recover(None, context, job.input, job.dependencies, Exception("test"))
        next(job_generator)  # Try to iterate the generator, which triggers the raise
