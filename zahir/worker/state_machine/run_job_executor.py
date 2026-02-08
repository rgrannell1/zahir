"""Shared job-execution logic.

Both execute_job and execute_recovery_job delegate here; only the
parametric differences (which timeout to read, which failure states
to emit, etc.) are passed in via an ``ExecutionParams`` dataclass.
"""

from dataclasses import dataclass
from math import ceil
import os
import signal
import traceback
from typing import cast

from zahir.base_types import JobState, validate_output_type
from zahir.events import Await, JobOutputEvent, JobWorkerWaitingEvent
from zahir.serialise import serialise_event
from zahir.worker.read_job_events import read_job_events
from zahir.worker.state_machine.states import (
    HandleAwaitStateChange,
    HandleJobCompleteNoOutputStateChange,
    HandleJobOutputStateChange,
    StateChange,
    WaitForJobStateChange,
)
from zahir.worker.state_machine.utils import process_await


def times_up(_signum, _frame):
    raise TimeoutError("Job execution timed out")


@dataclass(frozen=True)
class ExecutionParams:
    """The differences between normal and recovery execution."""

    # Label used in log/event messages, e.g. "Job" or "Recovery job"
    label: str

    # Whether to skip the set_state call when already RUNNING
    skip_if_already_running: bool

    # Which timing accessor to call on JobTimingInformation
    timing_method: str  # "time_since_started" or "time_since_recovery_started"

    # Which timeout field to read from JobArguments
    timeout_field: str  # "job_timeout" or "recover_timeout"

    # State-change class for exception outcomes
    exception_state: type[StateChange]

    # State-change class for timeout outcomes
    timeout_state: type[StateChange]


def run_job_executor(state, params: ExecutionParams):
    """Execute a job or recovery job, parameterised by *params*.

    Returns the (next-state-change, state) tuple expected by the
    state machine dispatcher.
    """

    current_state = state.context.job_registry.get_state(state.frame.job.job_id)

    if not (params.skip_if_already_running and current_state == JobState.RUNNING):
        state.context.job_registry.set_state(
            state.context,
            state.frame.job.job_id,
            state.frame.job.spec.type,
            state.workflow_id,
            state.output_queue,
            JobState.RUNNING,
            recovery=state.frame.recovery,
        )

    job_timing = state.context.job_registry.get_job_timing(state.frame.job.job_id)

    time_elapsed = getattr(job_timing, params.timing_method)() if job_timing else None
    job_timeout = getattr(state.frame.job.args, params.timeout_field)

    seconds_until_timeout = max(0, job_timeout - time_elapsed) if job_timeout and time_elapsed else job_timeout

    signal.signal(signal.SIGALRM, times_up)
    job_type = state.frame.job_type()
    label = params.label

    try:
        signal.alarm(ceil(seconds_until_timeout)) if seconds_until_timeout else None

        job_generator_result = read_job_events(
            job_registry=state.context.job_registry,
            output_queue=state.output_queue,
            state=state,
            workflow_id=state.workflow_id,
            job_id=cast(str, state.frame.job.job_id),
        )

        if isinstance(job_generator_result, JobOutputEvent):
            output_type = state.frame.job.spec.output_type
            postcheck_err = validate_output_type(job_generator_result.output, output_type)
            if postcheck_err is not None:
                state.last_event = postcheck_err
                return params.exception_state(
                    {"message": f"{label} {job_type} output validation failed: {postcheck_err}"},
                ), state

            state.last_event = job_generator_result
            return HandleJobOutputStateChange({"message": f"{label} {job_type} produced output"}), state

        if isinstance(job_generator_result, Await):
            state.await_event = process_await(job_generator_result)
            return HandleAwaitStateChange(
                {"message": f"{label} {job_type} is awaiting another job"},
            ), state

        if job_generator_result is None:
            return HandleJobCompleteNoOutputStateChange(
                {"message": f"{label} {job_type} completed with no output"},
            ), state

    except TimeoutError:
        return params.timeout_state(
            {"message": f"{label} {job_type} timed out"},
        ), state

    except Exception as err:
        state.last_event = err

        traceback_data = "".join(traceback.format_exception(type(err), err, err.__traceback__))

        return params.exception_state(
            {"message": f"{label} {job_type} raised exception of type '{type(err)}'\n{traceback_data}\n{err!s}"},
        ), state
    finally:
        signal.alarm(0)

    # Fallthrough — signal readiness for another job
    state.output_queue.put(
        serialise_event(state.context, JobWorkerWaitingEvent(pid=os.getpid(), workflow_id=state.workflow_id))
    )

    return WaitForJobStateChange({"message": f"{label} execution complete, waiting for next dispatch"}), state
