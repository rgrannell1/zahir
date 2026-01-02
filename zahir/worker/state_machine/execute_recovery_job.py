import signal
from typing import cast

from zahir.base_types import JobState
from zahir.events import Await, JobOutputEvent
from zahir.worker.read_job_events import read_job_events
from zahir.worker.state_machine.states import (
    EnqueueJobStateChange,
    HandleAwaitStateChange,
    HandleJobCompleteNoOutputStateChange,
    HandleJobOutputStateChange,
    HandleRecoveryJobExceptionStateChange,
    HandleRecoveryJobTimeoutStateChange,
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
    | EnqueueJobStateChange,
    None,
]:
    """Execute a recovery job. Similar to execute_job, but different eventing on failure/completion."""

    job_timeout = state.frame.job.options.job_timeout if state.frame.job.options else None

    # Emit a JobStartedEvent when we begin executing the claimed job.
    # Some job implementations don't emit this implicitly, so the
    # worker should surface it when execution begins.
    state.context.job_registry.set_state(
        state.frame.job.job_id, state.workflow_id, state.output_queue, JobState.RUNNING
    )

    signal.signal(signal.SIGALRM, times_up)
    signal.alarm(job_timeout) if job_timeout else None
    job_type = state.frame.job_type()

    try:
        job_generator_result = read_job_events(
            state.frame.job_generator,
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

    return EnqueueJobStateChange({"message": "Recovery execution complete, enqueueing"}), state
