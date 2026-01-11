import multiprocessing
import os
from typing import Any

from zahir.base_types import Context
from zahir.events import (
    Await,
    ZahirEvent,
)
from zahir.utils.logging_config import configure_logging, get_logger
from zahir.worker.call_frame import ZahirCallStack, ZahirStackFrame
from zahir.worker.state_machine.check_preconditions import check_preconditions
from zahir.worker.state_machine.execute_job import execute_job
from zahir.worker.state_machine.execute_recovery_job import execute_recovery_job
from zahir.worker.state_machine.handle_await import handle_await
from zahir.worker.state_machine.handle_job_complete_no_output import handle_job_complete_no_output
from zahir.worker.state_machine.handle_job_exception import handle_job_exception
from zahir.worker.state_machine.handle_job_output import handle_job_output
from zahir.worker.state_machine.handle_job_timeout import handle_job_timeout
from zahir.worker.state_machine.handle_recovery_job_complete_no_output import handle_recovery_job_complete_no_output
from zahir.worker.state_machine.handle_recovery_job_exception import handle_recovery_job_exception
from zahir.worker.state_machine.handle_recovery_job_timeout import handle_recovery_job_timeout
from zahir.worker.state_machine.pop_job import pop_job
from zahir.worker.state_machine.start import start
from zahir.worker.state_machine.states import (
    CheckPreconditionsStateChange,
    ExecuteJobStateChange,
    ExecuteRecoveryJobStateChange,
    HandleAwaitStateChange,
    HandleJobCompleteNoOutputStateChange,
    HandleJobExceptionStateChange,
    HandleJobOutputStateChange,
    HandleJobTimeoutStateChange,
    HandleRecoveryJobExceptionStateChange,
    HandleRecoveryJobTimeoutStateChange,
    PopJobStateChange,
    StartStateChange,
    WaitForJobStateChange,
    ZahirJobState,
)
from zahir.worker.state_machine.wait_for_job import wait_for_job

# Configure logging for this process
configure_logging()
log = get_logger(__name__)

GREEN = "\x1b[32m"
RESET = "\x1b[0m"


def times_up(_signum, _frame):
    raise TimeoutError("Job execution timed out")


type OutputQueue = multiprocessing.Queue["ZahirEvent"]


def log_call(fn):
    """Decorator to log function calls and their results."""

    def wrapper(*args, **kwargs):
        result = fn(*args, **kwargs)

        pid = os.getpid()

        message = f"{GREEN}{pid} {fn.__name__}"

        if isinstance(result, tuple):
            next_state = result[0]
            data = next_state.data

            message += f" → {next_state.state} | "
            message += data.get("message", "")

        message += RESET
        log.info(message)

        return result

    return wrapper


class ZahirWorkerState:
    """Mutable state held during job execution."""

    def __init__(
        self, context: Context, input_queue: "multiprocessing.Queue | None", output_queue: OutputQueue, workflow_id: str
    ):
        self.job_stack: ZahirCallStack = ZahirCallStack([])
        self.input_queue: multiprocessing.Queue | None = input_queue
        self.output_queue: OutputQueue = output_queue
        self.frame: ZahirStackFrame | None = None

        self.context: Context = context
        self.workflow_id: str = workflow_id
        self.await_event: Await | None = None

        # last_event holds a value (e.g. Await or JobOutputEvent) produced by a handler
        self.last_event: Any = None


class ZahirJobStateMachine:
    """Manages the actual state transitions of a Zahir job; run the thing,
    handle awaits, outputs, eventing, timeouts, failures, etc."""

    @classmethod
    @log_call
    def start(cls, state) -> tuple[WaitForJobStateChange | PopJobStateChange | CheckPreconditionsStateChange, None]:
        """Initial state; transition to waiting for a job from the overseer."""

        return start(state)

    @classmethod
    @log_call
    def wait_for_job(cls, state) -> tuple[StartStateChange | PopJobStateChange, None]:
        """Wait for the overseer to dispatch a job via the input queue."""

        return wait_for_job(state)

    @classmethod
    @log_call
    def pop_job(
        cls, state
    ) -> tuple[
        CheckPreconditionsStateChange
        | ExecuteJobStateChange
        | HandleRecoveryJobTimeoutStateChange
        | HandleJobTimeoutStateChange,
        None,
    ]:
        """We need a job; pop one off the stack"""

        return pop_job(state)

    @classmethod
    @log_call
    def check_preconditions(
        cls, state
    ) -> tuple[
        ExecuteRecoveryJobStateChange
        | ExecuteJobStateChange
        | StartStateChange
        | HandleRecoveryJobTimeoutStateChange
        | HandleJobTimeoutStateChange,
        None,
    ]:
        """Can we even run this job? Check the input preconditions first."""

        return check_preconditions(state)

    @classmethod
    @log_call
    def handle_await(cls, state) -> tuple[WaitForJobStateChange, None]:
        """We received an Await event. We should put out current job back on the stack,
        pause the job formally, then load the awaited job and start executing it."""

        return handle_await(state)

    @classmethod
    @log_call
    def handle_job_output(cls, state) -> tuple[StartStateChange, None]:
        """We received a job output! It's emitted upstream already; just null out the job state. Persist the output to the state if awaited; we'll pop, then pass
        the output to the awaiting job"""

        return handle_job_output(state)

    @classmethod
    @log_call
    def handle_job_complete_no_output(cls, state) -> tuple[WaitForJobStateChange, None]:
        """Mark the job as complete. Emit a completion event. Null out the job. Wait for next dispatch."""

        return handle_job_complete_no_output(state)

    @classmethod
    @log_call
    def handle_recovery_job_complete_no_output(cls, state) -> tuple[WaitForJobStateChange, None]:
        """Mark the recovery job as complete. Emit a recovery completion event. Null out the job. Wait for next dispatch."""

        return handle_recovery_job_complete_no_output(state)

    @classmethod
    @log_call
    def handle_job_timeout(cls, state) -> tuple[WaitForJobStateChange, None]:
        """The job timed out. Emit a timeout event, null out the job, and wait for next dispatch."""

        return handle_job_timeout(state)

    @classmethod
    @log_call
    def handle_recovery_job_timeout(cls, state) -> tuple[WaitForJobStateChange, None]:
        """The recovery job timed out. Emit a recovery timeout event, null out the job, and wait for next dispatch."""

        return handle_recovery_job_timeout(state)

    @classmethod
    @log_call
    def handle_recovery_job_exception(cls, state) -> tuple[WaitForJobStateChange, None]:
        """The recovery job raised an exception. Emit an irrecoverable event, null out the job, and wait for next dispatch."""

        return handle_recovery_job_exception(state)

    @classmethod
    @log_call
    def handle_job_exception(cls, state) -> tuple[CheckPreconditionsStateChange, None]:
        """The job raised an exception. Emit a recovery started event, and switch to recovery mode."""

        return handle_job_exception(state)

    @classmethod
    @log_call
    def execute_job(
        cls, state
    ) -> tuple[
        HandleJobOutputStateChange
        | HandleAwaitStateChange
        | HandleJobCompleteNoOutputStateChange
        | HandleJobTimeoutStateChange
        | HandleJobExceptionStateChange
        | WaitForJobStateChange,
        None,
    ]:
        """Execute a job. Handle timeouts, awaits, outputs, exceptions."""

        return execute_job(state)

    @classmethod
    def execute_recovery_job(
        cls, state
    ) -> tuple[
        HandleJobOutputStateChange
        | HandleAwaitStateChange
        | HandleJobCompleteNoOutputStateChange
        | HandleRecoveryJobTimeoutStateChange
        | HandleRecoveryJobExceptionStateChange
        | WaitForJobStateChange,
        None,
    ]:
        """Execute a recovery job. Similar to execute_job, but different eventing on failure/completion."""

        return execute_recovery_job(state)

    @classmethod
    def get_state(cls, state_name: ZahirJobState):
        return getattr(cls, state_name.value)
