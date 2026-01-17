from math import ceil
import os
import signal
from typing import cast

from zahir.base_types import JobState
from zahir.events import Await, JobOutputEvent, JobWorkerWaitingEvent
from zahir.serialise import serialise_event
from zahir.worker.read_job_events import read_job_events
from zahir.worker.state_machine.states import (
    HandleAwaitStateChange,
    HandleJobCompleteNoOutputStateChange,
    HandleJobOutputStateChange,
    HandleRecoveryJobExceptionStateChange,
    HandleRecoveryJobTimeoutStateChange,
    WaitForJobStateChange,
)


def times_up(_signum, _frame):
    raise TimeoutError("Job execution timed out")


def execute_recovery_job(
    state,
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

    # Emit a JobStartedEvent when we begin executing the claimed job.
    # Some job implementations don't emit this implicitly, so the
    # worker should surface it when execution begins.
    state.context.job_registry.set_state(
        state.frame.job.job_id, state.workflow_id, state.output_queue, JobState.RUNNING, recovery=state.frame.recovery
    )

    job_timing = state.context.job_registry.get_job_timing(state.frame.job.job_id)
    time_since_started = job_timing.time_since_recovery_started() if job_timing else None
    job_timeout = state.frame.job.args.recover_timeout

    seconds_until_timeout = (
        max(0, job_timeout - time_since_started) if job_timeout and time_since_started else job_timeout
    )

    signal.signal(signal.SIGALRM, times_up)
    job_type = state.frame.job_type()

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
            # store the event for the next handler to inspect. I think this should
            # be invariant for recover vs normal workflows.

            state.last_event = job_generator_result
            return HandleJobOutputStateChange(
                {"message": f"Recovery job {job_type} produced output"},
            ), state
        if isinstance(job_generator_result, Await):
            # our job is now awaiting another; switch to that before resuming the first one
            # recovery workflows are also allowed await, of course.
            state.await_event = job_generator_result
            return HandleAwaitStateChange(
                {"message": f"Recovery job {job_type} is awaiting another job"},
            ), state

        if job_generator_result is None:
            # differs between recovery and normal workflows
            return HandleJobCompleteNoOutputStateChange(
                {"message": f"Recovery job {job_type} completed with no output"},
            ), state

    except TimeoutError:
        return HandleRecoveryJobTimeoutStateChange(
            {"message": f"Recovery job {job_type} timed out"},
        ), state
    except Exception as err:
        state.last_event = err
        return HandleRecoveryJobExceptionStateChange(
            {"message": f"Recovery job {job_type} raised exception\n{err}"},
        ), state
    finally:
        signal.alarm(0)

    # Signal we're ready for another job (fallthrough case)
    state.output_queue.put(serialise_event(JobWorkerWaitingEvent(pid=os.getpid())))

    return WaitForJobStateChange({"message": "Recovery execution complete, waiting for next dispatch"}), state
