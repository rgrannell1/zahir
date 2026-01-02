import logging

from tblib import pickling_support  # type: ignore[import-untyped]

from zahir.base_types import Context, Job, JobRegistry, JobState
from zahir.events import (
    Await,
    JobOutputEvent,
    ZahirEvent,
)
from zahir.exception import JobPrecheckError, JobRecoveryTimeoutError, JobTimeoutError
from zahir.worker.call_frame import ZahirCallStack, ZahirStackFrame
from zahir.worker.state_machine.start import start
from zahir.worker.state_machine.states import (
    CheckPreconditionsStateChange,
    EnqueueJobStateChange,
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
    StateChange,
    WaitForJobStateChange,
    ZahirJobState,
)

pickling_support.install()

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from enum import StrEnum
import multiprocessing
import os
import signal
import time
from typing import Any, cast

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


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
    def start(cls, state) -> tuple[EnqueueJobStateChange | PopJobStateChange | CheckPreconditionsStateChange, None]:
        """Initial state; transition to enqueueing a job."""

        return start(state)

    @classmethod
    @log_call
    def enqueue_job(cls, state) -> tuple[StateChange, None]:
        """ """

        # TO-DO: this needs to be updated, so that paused jobs with satisfied awaits can be resumed.
        # We also need to claim new jobs while other jobs are stuck in pending. And keep track of ordering.

        # First, let's make sure we have at least one job, by adding it to the stack
        job = state.context.job_registry.claim(state.context, str(os.getpid()))

        if job is None:
            return WaitForJobStateChange({"message": "no pending job to claim, so waiting for one to appear"}), state

        job_generator = type(job).run(state.context, job.input, job.dependencies)
        frame = ZahirStackFrame(job=job, job_generator=job_generator, recovery=False)

        # new top-level job is not awaited by anything on the stack
        # and it's not recovering, it's healthy
        state.job_stack.push(frame)

        return StartStateChange({"message": "Appended new job to stack; going to start"}), state

    @classmethod
    @log_call
    def wait_for_job(cls, state) -> tuple[StateChange, None]:
        """No jobs available; for the moment let's just sleep. In future, be cleverer
        and have dependencies suggest nap-times"""

        time.sleep(1)

        return StartStateChange({"message": "Waited for job, restarting"}), state

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
            return CheckPreconditionsStateChange(
                {"message": f"Job {type(state.frame.job).__name__} claimed and active"},
            ), state

        return ExecuteJobStateChange(
            {"message": f"Resuming job {type(state.frame.job).__name__} in state '{job_state}'"},
        ), state

    @classmethod
    @log_call
    def check_preconditions(cls, state) -> tuple[StateChange, None]:
        """Can we even run this job? Check the input preconditions first."""

        errors = type(state.frame.job).precheck(state.frame.job.input)

        if not errors:
            if state.frame.recovery:
                return ExecuteRecoveryJobStateChange({"message": "Prechecks passed; executing recovery job"}), state
            return ExecuteJobStateChange({"message": "Prechecks passed; executing job"}), state

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

        return StartStateChange({"message": "Prechecks failed; cleared job."}), state

    @classmethod
    @log_call
    def handle_await(cls, state) -> tuple[StateChange, None]:
        """We received an Await event. We should put out current job back on the stack,
        pause the job formally, then load the awaited job and start executing it."""

        job_stack = state.job_stack
        job_registry = state.context.job_registry

        await_event = state.await_event
        assert await_event is not None

        # Update the paused job to await the new job(s)
        awaited_jobs = [await_event.job] if isinstance(await_event.job, Job) else await_event.job

        # Needed to handle types correctly in empty case
        if isinstance(await_event.job, list):
            state.frame.await_many = True

        # Tie the current job to the awaited job(s)
        for awaited_job in awaited_jobs:
            state.frame.required_jobs.add(awaited_job.job_id)

            # The awaited job can just be run through the normal lifecycle, with the caveat that the _awaiting_
            # job needs a marker thart it's awaiting the new job.
            job_registry.add(awaited_job, state.output_queue)

        # Pause the current job, and put it back on the registry. `Paused` jobs
        # are awaiting some job or other to be updated.
        frame_job_id = state.frame.job.job_id
        job_registry.set_state(frame_job_id, state.workflow_id, state.output_queue, JobState.PAUSED)
        job_stack.push(state.frame)
        state.frame = None

        return EnqueueJobStateChange(
            {"message": f"Paused job {frame_job_id}, enqueuing awaited job(s), and moving on to something else..."},
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

        return StartStateChange({"message": "Setting job output"}), state

    @classmethod
    @log_call
    def handle_job_complete_no_output(cls, state) -> tuple[StateChange, None]:
        """Mark the job as complete. Emit a completion event. Null out the job. Start over."""

        # Subjob complete, emit a recovery complete or regular compete and null out the stack frame.

        state.context.job_registry.set_state(
            state.frame.job.job_id, state.workflow_id, state.output_queue, JobState.COMPLETED
        )

        state.frame = None

        return EnqueueJobStateChange({"message": "Job completed with no output"}), state

    @classmethod
    @log_call
    def handle_recovery_job_complete_no_output(cls, state) -> tuple[StateChange, None]:
        """Mark the recovery job as complete. Emit a recovery completion event. Null out the job. Start over."""

        # Recovery subjob complete, emit a recovery complete and null out the stack frame.
        state.context.job_registry.set_state(
            state.frame.job.job_id, state.workflow_id, state.output_queue, JobState.RECOVERED
        )

        state.frame = None

        return EnqueueJobStateChange({"message": "Recovery job completed with no output"}), state

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

        return EnqueueJobStateChange({"message": f"Job {job_type} timed out"}), state

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

        return EnqueueJobStateChange({"message": f"Recovery job {job_type} timed out"}), state

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

        return EnqueueJobStateChange({"message": f"Recovery job {job_type} irrecoverably failed"}), state

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

        return CheckPreconditionsStateChange({"message": f"Job {job_type} entering recovery"}), state

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
                return HandleJobOutputStateChange({"message": f"Job {job_type} produced output"}), state
            if isinstance(job_generator_result, Await):
                # our job is now awaiting another; switch to that before resuming the first one
                # recovery workflows are also allowed await, of course.
                state.await_event = job_generator_result
                return HandleAwaitStateChange(
                    {"message": f"Job {job_type} is awaiting another job"},
                ), state

            if job_generator_result is None:
                # differs between recovery and normal workflows
                return HandleJobCompleteNoOutputStateChange(
                    {"message": f"Job {job_type} completed with no output"},
                ), state

        except TimeoutError:
            # differs between recovery and normal workflows

            return HandleJobTimeoutStateChange(
                {"message": f"Job {job_type} timed out"},
            ), state
        except Exception as err:
            # differs between recovery and normal workflows

            state.last_event = err
            return HandleJobExceptionStateChange(
                {"message": f"Job {job_type} raised exception\n{err}"},
            ), state
        finally:
            signal.alarm(0)

        return EnqueueJobStateChange({"message": "Execution complete, enqueueing"}), state

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

    required_pids = list(state.frame.required_jobs)

    if required_pids or state.frame.await_many:
        # Get the errors and outputs for the pid on which we were awaiting...

        # Not all jobs have to produce an output.
        outputs: list[Mapping | None] = []

        # Errors are 1:{0,1,...} per job
        errors: list[BaseException] = []

        # Collate the information for each job
        for required_pid in required_pids:
            outputs.append(job_registry.get_output(required_pid))
            job_errors = job_registry.get_errors(required_pid)
            errors += job_errors

        # So, this sucks. I ported from a custom serialiser to tblib. Turns out tblib
        # can't handle throwing into generators. We'll need to port back to a custom serialiser (screw pickle anyway)
        # to get our error-types and traces back.

        throwable_errors = []
        for error in errors:
            throwable_errors.append(Exception(str(error)))

        if errors:
            # Try to throw one exception, but let's not bury information either if there's multiple.
            throwable = (
                throwable_errors[0]
                if len(throwable_errors) == 1
                # we might want a custom ExceptionGroup type
                else ExceptionGroup("Multiple errors", throwable_errors)
            )

            event = state.frame.job_generator.throw(throwable)
        else:
            generator_input = None
            generator_input = outputs[0] if len(required_pids) == 1 else outputs
            event = state.frame.job_generator.send(generator_input)

        # something happens afterwards, so enqueue it and proceed
        queue.append(event)

        # we might have further awaits, so tidy up the state so we can await yet again!
        state.frame.required_jobs = set()

    while True:
        # deal with the `send` or `throw` event, if we have one
        item = queue.pop(0) if queue else None

        if item is None:
            try:
                item = next(gen)
            except StopIteration:
                # We're finished!
                return None

        if isinstance(item, Await):
            return item

        if isinstance(item, ZahirEvent) and hasattr(item, "workflow_id"):
            cast(Any, item).workflow_id = workflow_id

        if isinstance(item, ZahirEvent) and hasattr(item, "job_id"):
            cast(Any, item).job_id = job_id

        output_queue.put(item)

        if isinstance(item, JobOutputEvent):
            # Nothing more to be done for this generator
            return item

        elif isinstance(item, Job):
            # new subjob, yield as a serialised event upstream
            job_registry.add(item, output_queue)
            continue
