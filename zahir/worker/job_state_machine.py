from tblib import pickling_support  # type: ignore[import-untyped]

from zahir.base_types import Context, Job, JobRegistry, JobState
from zahir.events import (
    Await,
    JobCompletedEvent,
    JobIrrecoverableEvent,
    JobOutputEvent,
    JobPrecheckFailedEvent,
    JobRecoveryStartedEvent,
    JobRecoveryTimeoutEvent,
    JobStartedEvent,
    JobTimeoutEvent,
    WorkflowOutputEvent,
    ZahirCustomEvent,
    ZahirEvent,
)
from zahir.exception import JobPrecheckError, JobRecoveryTimeoutError, JobTimeoutError
from zahir.worker.frame import ZahirCallStack, ZahirStackFrame

pickling_support.install()

from collections.abc import Iterator
from dataclasses import dataclass
from enum import StrEnum
import json
import multiprocessing
import os
import signal
import time
from typing import Any, cast


class ZahirJobState(StrEnum):
    """Zahir transitions through these states when executing jobs. States are generally 'deconditionalised';
    when we transition out we transition to a state; the state doesn't if-else on the input in general. This
    makes things more traceable, but leads to duplicated states."""

    START = "start"

    ENQUEUE_JOB = "enqueue_job"
    WAIT_FOR_JOB = "wait_for_job"
    POP_JOB = "pop_job"

    CHECK_PRECONDITIONS = "check_preconditions"
    EXECUTE_JOB = "execute_job"
    EXECUTE_RECOVERY_JOB = "execute_recovery_job"
    HANDLE_AWAIT = "handle_await"

    HANDLE_JOB_OUTPUT = "handle_job_output"

    HANDLE_JOB_COMPLETE_NO_OUTPUT = "handle_job_complete_no_output"
    HANDLE_RECOVERY_JOB_COMPLETE_NO_OUTPUT = "handle_recovery_job_complete_no_output"

    HANDLE_JOB_TIMEOUT = "handle_job_timeout"
    HANDLE_JOB_EXCEPTION = "handle_job_exception"

    HANDLE_RECOVERY_JOB_TIMEOUT = "handle_recovery_job_timeout"
    HANDLE_RECOVERY_JOB_EXCEPTION = "handle_recovery_job_exception"


@dataclass
class StateChange:
    """Represents a state transition in the Zahir job state machine."""

    state: ZahirJobState
    data: dict


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
            message += f" -> {next_state.state}({json.dumps(next_state.data)})"

        message += RESET
        print(message)

        return result

    return wrapper


class ZahirWorkerState:
    """Mutable state held during job execution."""

    def __init__(self, context: Context, output_queue: OutputQueue, workflow_id: str):
        self.job_stack: ZahirCallStack = ZahirCallStack([])
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
    def start(cls, state) -> tuple[StateChange, None]:
        """Initial state; transition to enqueueing a job."""

        if not state.frame:
            # There is no active job; let's either pop one from the stack if something is runnable or enqueue a new one.
            if state.job_stack.is_empty():
                return StateChange(ZahirJobState.ENQUEUE_JOB, {"message": "No job; enqueueing."}), state

            # We _could_ run this job by popping it of the frame. It's possible we'll actually run a
            # different one when we pop a job, which is also fine.
            runnable_frame_idx = state.job_stack.runnable_frame_idx(state.context.job_registry)

            if runnable_frame_idx is not None:
                return StateChange(ZahirJobState.POP_JOB, {"message": "No job active, so popping from stack"}), state
            else:
                return StateChange(ZahirJobState.ENQUEUE_JOB, {"message": "No job runnable; enqueueing."}), state

        job_type = state.frame.job_type()

        return StateChange(
            ZahirJobState.CHECK_PRECONDITIONS,
            {"message": f"Checking preconditions for '{job_type}'"},
        ), state

    @classmethod
    @log_call
    def enqueue_job(cls, state) -> tuple[StateChange, None]:
        """ """

        # TO-DO: this needs to be updated, so that paused jobs with satisfied awaits can be resumed.
        # We also need to claim new jobs while other jobs are stuck in pending. And keep track of ordering.

        # First, let's make sure we have at least one job, by adding it to the stack
        job = state.context.job_registry.claim(state.context, str(os.getpid()))

        if job is None:
            return StateChange(
                ZahirJobState.WAIT_FOR_JOB, {"message": "no pending job to claim, so waiting for one to appear"}
            ), state

        job_generator = type(job).run(state.context, job.input, job.dependencies)
        frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)

        # new top-level job is not awaited by anything on the stack
        # and it's not recovering, it's healthy
        state.job_stack.push(frame)

        return StateChange(ZahirJobState.START, {"message": "Appended new job to stack; going to start"}), state

    @classmethod
    @log_call
    def wait_for_job(cls, state) -> tuple[StateChange, None]:
        """No jobs available; for the moment let's just sleep. In future, be cleverer
        and have dependencies suggest nap-times"""

        time.sleep(1)

        return StateChange(ZahirJobState.START, {"message": "Waited for job, restarting"}), state

    @classmethod
    @log_call
    def pop_job(cls, state) -> tuple[StateChange, None]:
        """We need a job; pop one off the stack"""

        # different one when we pop a job, which is also fine.
        runnable_frame_idx = state.job_stack.runnable_frame_idx(state.context.job_registry)
        assert runnable_frame_idx is not None

        state.frame = state.job_stack.pop(runnable_frame_idx)

        job_state = state.context.job_registry.get_state(state.frame.job.job_id)

        if job_state == JobState.READY:
            return StateChange(
                ZahirJobState.CHECK_PRECONDITIONS,
                {"message": f"Job {type(state.frame.job).__name__} claimed and active"},
            ), state

        return StateChange(
            ZahirJobState.EXECUTE_JOB,
            {"message": f"Resuming job {type(state.frame.job).__name__} in state '{job_state}'"},
        ), state

    @classmethod
    @log_call
    def check_preconditions(cls, state) -> tuple[StateChange, None]:
        """Can we even run this job? Check the input preconditions first."""

        errors = type(state.frame.job).precheck(state.frame.job.input)

        if not errors:
            if state.frame.recovery:
                return StateChange(
                    ZahirJobState.EXECUTE_RECOVERY_JOB, {"message": "Prechecks passed; executing recovery job"}
                ), state
            return StateChange(ZahirJobState.EXECUTE_JOB, {"message": "Prechecks passed; executing job"}), state

        # we'll avoid re-running for `Paused` jobs.
        # Precheck failed; job is no longer on the stack, so
        # let's report and continue.

        precheck_error = JobPrecheckError("\n".join(errors))

        state.context.job_registry.set_state(
            state.frame.job.job_id,
            state.workflow_id,
            state.output_queue,
            JobState.PRECHECK_FAILED,
            error=precheck_error,
        )

        state.frame = None

        return StateChange(ZahirJobState.START, {"message": "Prechecks failed; cleared job."}), state

    @classmethod
    @log_call
    def handle_await(cls, state) -> tuple[StateChange, None]:
        """We received an Await event. We should put out current job back on the stack,
        pause the job formally, then load the awaited job and start executing it."""

        job_stack = state.job_stack
        job_registry = state.context.job_registry

        await_event = state.await_event
        assert await_event is not None

        # Update the paused job to await the new job
        awaited_job_id = await_event.job.job_id
        state.frame.required_jobs.add(awaited_job_id)

        # Pause the current job, and put it back on the registry. `Paused` jobs
        # are awaiting some job or other to be updated.
        frame_job_id = state.frame.job.job_id
        job_registry.set_state(frame_job_id, state.workflow_id, state.output_queue, JobState.PAUSED)
        job_stack.push(state.frame)
        state.frame = None

        new_awaited_job = await_event.job

        # The awaited job can just be run through the normal lifecycle, with the caveat that the _awaiting_
        # job needs a marker thart it's awaiting the new job.
        job_registry.add(new_awaited_job, state.output_queue)

        return StateChange(
            ZahirJobState.ENQUEUE_JOB,
            {
                "message": f"Paused job {frame_job_id}, enqueuing awaited job {awaited_job_id}, and moving on to something else..."
            },
        ), state

    @classmethod
    @log_call
    def handle_job_output(cls, state) -> tuple[StateChange, None]:
        """We received a job output! It's emitted upstream already; just null out the job state. Persist the output to the state if awaited; we'll pop, then pass
        the output to the awaiting job"""

        # silly to store twice
        state.context.job_registry.set_output(
            state.frame.job.job_id, state.workflow_id, state.output_queue, state.last_event.output
        )

        # Jobs can only output once, so clear the job and generator
        state.frame = None

        return StateChange(ZahirJobState.START, {"message": "Setting job output"}), state

    @classmethod
    @log_call
    def handle_job_complete_no_output(cls, state) -> tuple[StateChange, None]:
        """Mark the job as complete. Emit a completion event. Null out the job. Start over."""

        # Subjob complete, emit a recovery complete or regular compete and null out the stack frame.

        state.context.job_registry.set_state(
            state.frame.job.job_id, state.workflow_id, state.output_queue, JobState.COMPLETED
        )

        state.frame = None

        return StateChange(ZahirJobState.ENQUEUE_JOB, {"message": "Job completed with no output"}), state

    @classmethod
    @log_call
    def handle_recovery_job_complete_no_output(cls, state) -> tuple[StateChange, None]:
        """Mark the recovery job as complete. Emit a recovery completion event. Null out the job. Start over."""

        # Recovery subjob complete, emit a recovery complete and null out the stack frame.
        state.context.job_registry.set_state(
            state.frame.job.job_id, state.workflow_id, state.output_queue, JobState.RECOVERED
        )

        state.frame = None

        return StateChange(ZahirJobState.ENQUEUE_JOB, {"message": "Recovery job completed with no output"}), state

    @classmethod
    @log_call
    def handle_job_timeout(cls, state) -> tuple[StateChange, None]:
        """The job timed out. Emit a timeout event, null out the job, and start over."""

        error = JobTimeoutError("Job execution timed out")

        state.context.job_registry.set_state(
            state.frame.job.job_id, state.workflow_id, state.output_queue, JobState.TIMED_OUT, error=error
        )

        job_type = state.frame.job_type()
        state.frame = None

        return StateChange(ZahirJobState.ENQUEUE_JOB, {"message": f"Job {job_type} timed out"}), state

    @classmethod
    @log_call
    def handle_recovery_job_timeout(cls, state) -> tuple[StateChange, None]:
        """The recovery job timed out. Emit a recovery timeout event, null out the job, and start over."""

        error = JobRecoveryTimeoutError("Recovery job execution timed out")

        state.context.job_registry.set_state(
            state.frame.job.job_id, state.workflow_id, state.output_queue, JobState.RECOVERY_TIMED_OUT, error=error
        )
        job_type = state.frame.job_type()
        state.frame = None

        return StateChange(ZahirJobState.ENQUEUE_JOB, {"message": f"Recovery job {job_type} timed out"}), state

    @classmethod
    @log_call
    def handle_recovery_job_exception(cls, state) -> tuple[StateChange, None]:
        """The recovery job raised an exception. Emit an irrecoverable event, null out the job, and start over."""

        # well, recovery didn't work. Ah, well.
        state.context.job_registry.set_state(
            state.frame.job.job_id,
            state.workflow_id,
            state.output_queue,
            JobState.IRRECOVERABLE,
            error=state.last_event,
        )

        job_type = state.frame.job_type()
        state.frame = None

        return StateChange(
            ZahirJobState.ENQUEUE_JOB, {"message": f"Recovery job {job_type} irrecoverably failed"}
        ), state

    @classmethod
    @log_call
    def handle_job_exception(cls, state) -> tuple[StateChange, None]:
        """The job raised an exception. Emit a recovery started event, and switch to recovery mode."""

        job_class = type(state.frame.job)

        # Let's fork execution back to the job's recovery mechanism
        # we keep the same process ID. We should update the job registry.
        state.frame.job_generator = job_class.recover(
            state.context, state.frame.job.input, state.frame.job.dependencies, state.last_event
        )

        state.frame.recovery = True
        state.context.job_registry.set_state(
            state.frame.job.job_id, state.workflow_id, state.output_queue, JobState.RECOVERING
        )
        job_type = state.frame.job_type()

        return StateChange(ZahirJobState.CHECK_PRECONDITIONS, {"message": f"Job {job_type} entering recovery"}), state

    @classmethod
    @log_call
    def execute_job(cls, state) -> tuple[StateChange, None]:
        """Execute a job. Handle timeouts, awaits, outputs, exceptions."""

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
                return StateChange(
                    ZahirJobState.HANDLE_JOB_OUTPUT, {"message": f"Job {job_type} produced output"}
                ), state
            if isinstance(job_generator_result, Await):
                # our job is now awaiting another; switch to that before resuming the first one
                # recovery workflows are also allowed await, of course.
                state.await_event = job_generator_result
                return StateChange(
                    ZahirJobState.HANDLE_AWAIT,
                    {"message": f"Job {job_type} is awaiting another job"},
                ), state

            if job_generator_result is None:
                # differs between recovery and normal workflows
                return StateChange(
                    ZahirJobState.HANDLE_JOB_COMPLETE_NO_OUTPUT,
                    {"message": f"Job {job_type} completed with no output"},
                ), state

        except TimeoutError:
            # differs between recovery and normal workflows

            return StateChange(ZahirJobState.HANDLE_JOB_TIMEOUT, {"message": f"Job {job_type} timed out"}), state
        except Exception as err:
            # differs between recovery and normal workflows

            state.last_event = err
            return StateChange(
                ZahirJobState.HANDLE_JOB_EXCEPTION,
                {"message": f"Job {job_type} raised exception\n{err}"},
            ), state
        finally:
            signal.alarm(0)

        return StateChange(ZahirJobState.ENQUEUE_JOB, {"message": "Execution complete, enqueueing"}), state

    @classmethod
    def execute_recovery_job(cls, state) -> tuple[StateChange, None]:
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
                return StateChange(
                    ZahirJobState.HANDLE_JOB_OUTPUT,
                    {"message": f"Recovery job {job_type} produced output"},
                ), state
            if isinstance(job_generator_result, Await):
                # our job is now awaiting another; switch to that before resuming the first one
                # recovery workflows are also allowed await, of course.
                state.await_event = job_generator_result
                return StateChange(
                    ZahirJobState.HANDLE_AWAIT,
                    {"message": f"Recovery job {job_type} is awaiting another job"},
                ), state

            if job_generator_result is None:
                # differs between recovery and normal workflows
                return StateChange(
                    ZahirJobState.HANDLE_RECOVERY_JOB_COMPLETE_NO_OUTPUT,
                    {"message": f"Recovery job {job_type} completed with no output"},
                ), state

        except TimeoutError:
            return StateChange(
                ZahirJobState.HANDLE_RECOVERY_JOB_TIMEOUT,
                {"message": f"Recovery job {job_type} timed out"},
            ), state
        except Exception as err:
            state.last_event = err
            return StateChange(
                ZahirJobState.HANDLE_RECOVERY_JOB_EXCEPTION,
                {"message": f"Recovery job {job_type} raised exception\n{err}"},
            ), state
        finally:
            signal.alarm(0)

        return StateChange(ZahirJobState.ENQUEUE_JOB, {"message": "Recovery execution complete, enqueueing"}), state

    @classmethod
    def get_state(cls, state_name: ZahirJobState):
        return getattr(cls, state_name.value)


def read_job_events(
    gen: Iterator,
    *,
    job_registry: JobRegistry,
    output_queue: OutputQueue,
    state: ZahirWorkerState,
    workflow_id: str,
    job_id: str,
) -> Await | JobOutputEvent | None:
    """Sent job output items to the output queue."""

    if not isinstance(gen, Iterator):
        raise TypeError("gen must be an Iterator")

    queue: list[ZahirEvent | Job] = []

    assert state.frame is not None

    # TO-DO: support awaitmany semantics by collecting inputs
    required_pids = list(state.frame.required_jobs)
    required_pid = required_pids[0] if required_pids else None

    # TO-DO: this isn't picking up dependency issues

    if required_pid:
        # Get the errors and outputs for the pid on which we were waiting...
        output = job_registry.get_output(required_pid)
        errors = job_registry.get_errors(required_pid)

        # send or throw into the generator
        if errors:
            # mystery tblib error. The job hangs when I throw a wrapped
            # exception into the generator. So, unwrap and rethrow.
            # THis is very bad, never use pickle...
            not_throwable = errors[-1]

            event = state.frame.job_generator.throw(Exception(str(not_throwable)))
            pickling_support.install()
        else:
            event = state.frame.job_generator.send(output)

        # something happens afterwards, so enqueue it and proceed
        queue.append(event)

        # we might have further awaits, so tidy up the state so we can await yet again!
        state.frame.required_jobs = set()

    while True:
        # deal with the send event, if we have one
        item = queue.pop(0) if queue else None

        if item is None:
            try:
                item = next(gen)
            except StopIteration:
                # We're finished!
                return None

        if isinstance(item, Await):
            return item

        if isinstance(
            item,
            (
                JobOutputEvent,
                WorkflowOutputEvent,
                JobCompletedEvent,
                JobPrecheckFailedEvent,
                JobRecoveryStartedEvent,
                JobRecoveryTimeoutEvent,
                JobStartedEvent,
                JobTimeoutEvent,
                ZahirCustomEvent,
                JobIrrecoverableEvent,
            ),
        ):
            item.workflow_id = workflow_id
            item.job_id = job_id

            output_queue.put(item)

            if isinstance(item, JobOutputEvent):
                # Nothing more to be done for this generator
                return item

        elif isinstance(item, Job):
            # new subjob, yield as a serialised event upstream
            job_registry.add(item, output_queue)
            continue
