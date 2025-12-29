from collections.abc import Iterator
from concurrent.futures import (
    ThreadPoolExecutor,
    TimeoutError as FutureTimeoutError,
)
from dataclasses import dataclass
from enum import StrEnum
import json
import multiprocessing
import os
import time
from typing import cast

from zahir.base_types import Context, Job, JobState
from zahir.events import (
    Await,
    JobCompletedEvent,
    JobEvent,
    JobIrrecoverableEvent,
    JobOutputEvent,
    JobPausedEvent,
    JobPrecheckFailedEvent,
    JobRecoveryCompletedEvent,
    JobRecoveryStarted,
    JobRecoveryTimeout,
    JobStartedEvent,
    JobTimeoutEvent,
    WorkflowOutputEvent,
    ZahirCustomEvent,
    ZahirEvent,
)
from zahir.exception import JobPrecheckError


class ZahirJobState(StrEnum):
    """Zahir transitions through these states when executing jobs."""

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
    state: ZahirJobState
    data: dict


type OutputQueue = multiprocessing.Queue["ZahirEvent"]

GREEN = "\x1b[32m"
RESET = "\x1b[0m"

def log_call(fn):
    def wrapper(*args, **kwargs):
        result = fn(*args, **kwargs)

        pid = os.getpid()

        message = (
            f"{GREEN}"
            f"{pid} "
            f"{fn.__name__}"
        )

        if isinstance(result, tuple):
            next_state = result[0]
            message += f" -> {next_state.state}({json.dumps(next_state.data)})"

        message += RESET
        print(message)

        return result

    return wrapper

# TODO update to just state.frame to simplify context switches
class ZahirWorkerState:
    """Mutable state held during job execution."""

    def __init__(self, context: Context, output_queue: OutputQueue, workflow_id: str):
        self.job: Job | None = None
        self._job_gen: Iterator | None = None
        # each entry is (job, job_gen, awaited: bool)
        self.run_job_stack: list[ZahirStackFrame] = []
        self.context: Context = context
        self.output_queue: OutputQueue = output_queue
        self.workflow_id: str = workflow_id
        self.await_event: Await | None = None

        self.job_errors: dict[str, Exception] = {}
        self.job_outputs: dict[str, dict] = {}
        self.awaiting_jobs: dict[str, str] = {}

        self.frame : ZahirStackFrame | None = None
        self.recovery: bool = False

        # last_event holds a value (e.g. Await or JobOutputEvent) produced by a handler
        # Dubious LLM suggestion, keeping it for now.
        self.last_event: any = None

        self.current_job_wants_output_sent: bool = False

    @property
    def job_gen(self) -> Iterator | None:
        return self._job_gen

    @job_gen.setter
    def job_gen(self, value: Iterator | None) -> None:
        if not isinstance(value, Iterator) and value is not None:
            message = "job_gen must be an Iterator or None"

            if self.job:
                message += f" does {type(self.job)} return instead of yield?"

            raise TypeError(message)

        self._job_gen = value

@dataclass
class ZahirStackFrame:
    """An instantiated job, its run or recovery generator, and whether it's awaited.

    Zahir job workers maintain a stack of call-frames for nested job execution.
    """

    job: Job
    job_gen: Iterator
    awaited: bool
    recovery: bool = False

class ZahirJobStateMachine:
    """Manages the actual state transitions of a Zahir job; run the thing,
    handle awaits, outputs, eventing, timeouts, failures, etc."""

    @classmethod
    @log_call
    def start(cls, state) -> tuple[StateChange, None]:
        """Initial state; transition to enqueueing a job."""

        if not state.job or not state.job_gen:
            if not state.run_job_stack:
                return StateChange(ZahirJobState.ENQUEUE_JOB, {
                    "message": "No job; enqueueing."
                }), state

            return StateChange(ZahirJobState.POP_JOB, {
                "message": "No job active, so popping from stack"
            }), state
        else:
            return StateChange(ZahirJobState.CHECK_PRECONDITIONS, {
                "message": f"Checking preconditions for '{state.job.__class__.__name__}'"
            }), state

    @classmethod
    @log_call
    def enqueue_job(cls, state) -> tuple[StateChange, None]:
        """ """

        # First, let's make sure we have at least one job, by adding it to the stack
        job = state.context.job_registry.claim(state.context)
        if job is None:
            return StateChange(ZahirJobState.WAIT_FOR_JOB, {
                "message": "no pending job to claim, so waiting for one to appear"
            }), state

        job_gen = type(job).run(state.context, job.input, job.dependencies)
        # new top-level job is not awaited by anything on the stack
        state.run_job_stack.append(ZahirStackFrame(
            job=job,
            job_gen=job_gen,
            awaited=False,
            recovery=False
        ))

        return StateChange(ZahirJobState.START, {
            "message": "Appended new job to stack; going to start"
        }), state

    @classmethod
    @log_call
    def wait_for_job(cls, state) -> tuple[StateChange, None]:
        """No jobs available; for the moment let's just sleep. In future, be cleverer
        and have dependencies suggest nap-times"""
        time.sleep(1)

        return StateChange(ZahirJobState.START, {
            "message": "Waited for job, restarting"
        }), state

    @classmethod
    @log_call
    def pop_job(cls, state) -> tuple[StateChange, None]:
        """We need a job; pop one off the stack"""

        frame = state.run_job_stack.pop()
        state.frame = frame

        state.job, state.job_gen = frame.job, frame.job_gen
        state.current_job_wants_output_sent = frame.awaited

        job_state =  state.context.job_registry.get_state(state.frame.job.job_id)

        if job_state == JobState.CLAIMED:
            return StateChange(ZahirJobState.CHECK_PRECONDITIONS, {
                "message": "Job claimed and active"
            }), state
        else:
            return StateChange(ZahirJobState.EXECUTE_JOB, {
                "message": f"Resuming job in state '{job_state}'"
            }), state

    @classmethod
    @log_call
    def check_preconditions(cls, state) -> tuple[StateChange, None]:
        """Can we even run this job? Check the input preconditions first."""

        errors = type(state.job).precheck(state.job.input)

        if not errors:
            if state.recovery:
                return StateChange(ZahirJobState.EXECUTE_RECOVERY_JOB, {
                    "message": "Prechecks passed; executing recovery job"
                }), state
            else:
                return StateChange(ZahirJobState.EXECUTE_JOB, {
                    "message": "Prechecks passed; executing job"
                }), state

        # we'll avoid re-running for `Paused` jobs.
        # Precheck failed; job is no longer on the stack, so
        # let's report and continue.

        state.output_queue.put(
            JobPrecheckFailedEvent(
                workflow_id=state.workflow_id,
                job_id=state.job.job_id,
                errors=errors,
            )
        )
        state.job_errors[state.job.job_id] = JobPrecheckError("\n".join(errors))
        state.job = None
        state.job_gen = None
        state.frame = None

        return StateChange(ZahirJobState.START, {
            "message": "Prechecks failed; cleared job."
        }), state


    @classmethod
    @log_call
    def handle_await(cls, state) -> tuple[StateChange, None]:
        """We received an Await event. We should put out current job back on the stack,
        pause the job formally, then load the awaited job and start executing it."""

        # Pause the current job, put it back on the worker-process stack and mark it awaited.
        state.run_job_stack.append(ZahirStackFrame(
            job=state.job,
            job_gen=state.job_gen,
            awaited=True,
            recovery=state.recovery
        ))

        # Current job awaits new job
        state.awaiting_jobs[state.job.job_id] = state.await_event.job.job_id

        # Pause the job please
        state.output_queue.put(JobPausedEvent(workflow_id=state.workflow_id, job_id=state.job.job_id))

        await_event = state.await_event
        assert await_event is not None

        job = await_event.job
        state.context.job_registry.add(job)
        # TO DO write the Job Event; I think we probably need to decouple eventing from registry updates. It's pretty, sane, and extremely non-deterministic to use eventing for updates.
        state.context.job_registry.set_state(job.job_id, JobState.CLAIMED)

        state.output_queue.put(JobStartedEvent(workflow_id=state.workflow_id, job_id=job.job_id))

        # ...then, let's instantiate the `run` generator
        job_gen = type(job).run(state.context, job.input, job.dependencies)
        state.job, state.job_gen = job, job_gen

        # this new job is not awaited yet (it's the awaited target)
        state.current_job_wants_output_sent = False

        return StateChange(ZahirJobState.CHECK_PRECONDITIONS, {
            "message": f"Checking preconditions for awaited job '{type(job).__name__}'"
        }), state

    @classmethod
    @log_call
    def handle_job_output(cls, state) -> tuple[StateChange, None]:
        """We received a job output! It's emitted upstream already; just null out the job state. Persist the output to the state if awaited; we'll pop, then pass
        the output to the awaiting job"""

        state.job_outputs[state.job.job_id] = state.last_event.output

        # Jobs can only output once, so clear the job and generator
        state.job = None
        state.job_gen = None

        return StateChange(ZahirJobState.START, {
            "message": "Setting job output"
        }), state

    @classmethod
    @log_call
    def handle_job_complete_no_output(cls, state) -> tuple[StateChange, None]:
        """Mark the job as complete. Emit a completion event. Null out the job. Start over."""

        # Subjob complete, emit a recovery complete or regular compete and null out the stack frame.
        state.output_queue.put(
            JobCompletedEvent(
                workflow_id=state.workflow_id,
                job_id=state.job.job_id,
                duration_seconds=0.1,
            )
        )

        state.job = None
        state.job_gen = None

        return StateChange(ZahirJobState.ENQUEUE_JOB, {
            "message": "Job completed with no output"
        }), state

    @classmethod
    @log_call
    def handle_recovery_job_complete_no_output(cls, state) -> tuple[StateChange, None]:
        """Mark the recovery job as complete. Emit a recovery completion event. Null out the job. Start over."""

        # Recovery subjob complete, emit a recovery complete and null out the stack frame.
        state.output_queue.put(
            JobRecoveryCompletedEvent(
                workflow_id=state.workflow_id,
                job_id=state.job.job_id,
                duration_seconds=0.1,
            )
        )

        state.job = None
        state.job_gen = None

        return StateChange(ZahirJobState.ENQUEUE_JOB, {
            "message": "Recovery job completed with no output"
        }), state

    @classmethod
    @log_call
    def handle_job_timeout(cls, state) -> tuple[StateChange, None]:
        job_timeout = state.job.options.job_timeout if state.job.options else None

        state.output_queue.put(
            JobTimeoutEvent(
                workflow_id=state.workflow_id,
                job_id=state.job.job_id,
                duration_seconds=cast(float, job_timeout),
            )
        )

        state.job = None
        state.job_gen = None

        return StateChange(ZahirJobState.ENQUEUE_JOB, {
            "message": f"Job {type(state.job).__name__} timed out"
        }), state

    @classmethod
    @log_call
    def handle_recovery_job_timeout(cls, state) -> tuple[StateChange, None]:
        recovery_timeout = state.job.recovery_timeout if state.job.options else None

        state.output_queue.put(
            JobRecoveryTimeout(
                workflow_id=state.workflow_id,
                job_id=state.job.job_id,
                duration_seconds=cast(float, recovery_timeout),
            )
        )

        state.job = None
        state.job_gen = None

        return StateChange(ZahirJobState.ENQUEUE_JOB, {
            "message": f"Recovery job {type(state.job).__name__} timed out"
        }), state

    @classmethod
    @log_call
    def handle_recovery_job_exception(cls, state) -> tuple[StateChange, None]:
        # well, recovery didn't work. Ah, well.

        state.output_queue.put(
            JobIrrecoverableEvent(
                workflow_id=state.workflow_id,
                error= state.last_event,
                job_id= state.job.job_id
            )
        )
        state.job = None
        state.job_gen = None
        state.recovery = False

        return StateChange(ZahirJobState.ENQUEUE_JOB, {
            "message": f"Recovery job {type(state.job).__name__} irrecoverably failed"
        }), state

    @classmethod
    @log_call
    def handle_job_exception(cls, state) -> tuple[StateChange, None]:
        job_class = type(state.job)

        # Let's fork execution back to the job's recovery mechanism
        # we keep the same process ID. We should update the job registry.
        state.job_gen = job_class.recover(state.context, state.job.input, state.job.dependencies, state.last_event)

        state.recovery = True
        state.context.job_registry.set_state(state.job.job_id, JobState.RECOVERING)

        return StateChange(ZahirJobState.CHECK_PRECONDITIONS, {
            "message": f"Job {type(state.job).__name__} entering recovery"
        }), state

    @classmethod
    @log_call
    def execute_job(cls, state) -> tuple[StateChange, None]:
        job_timeout = state.job.options.job_timeout if state.job.options else None

        # Emit a JobStartedEvent when we begin executing the claimed job.
        # Some job implementations don't emit this implicitly, so the
        # worker should surface it when execution begins.
        state.output_queue.put(JobStartedEvent(workflow_id=state.workflow_id, job_id=state.job.job_id))

        with ThreadPoolExecutor(max_workers=1) as ex:
            try:
                job_gen_result = ex.submit(
                    handle_job_events,
                    state.job_gen,
                    output_queue=state.output_queue,
                    state=state,
                    workflow_id=state.workflow_id,
                    job_id=cast(str, state.job.job_id),
                ).result(timeout=job_timeout)

                if isinstance(job_gen_result, JobOutputEvent):
                    # store the event for the next handler to inspect. I think this should
                    # be invariant for recover vs normal workflows.

                    state.last_event = job_gen_result
                    return StateChange(ZahirJobState.HANDLE_JOB_OUTPUT, {
                        "message": f"Job {type(state.job).__name__} produced output"
                    }), state
                if isinstance(job_gen_result, Await):
                    # our job is now awaiting another; switch to that before resuming the first one
                    # recovery workflows are also allowed await, of course.
                    state.await_event = job_gen_result
                    return StateChange(ZahirJobState.HANDLE_AWAIT, {
                        "message": f"Job {type(state.job).__name__} is awaiting another job"
                    }), state

                if job_gen_result is None:
                    # differs between recovery and normal workflows
                    return StateChange(ZahirJobState.HANDLE_JOB_COMPLETE_NO_OUTPUT, {
                        "message": f"Job {type(state.job).__name__} completed with no output"
                    }), state

            except FutureTimeoutError:
                # differs between recovery and normal workflows

                return StateChange(ZahirJobState.HANDLE_JOB_TIMEOUT, {
                    "message": f"Job {type(state.job).__name__} timed out"
                }), state
            except Exception as err:
                # differs between recovery and normal workflows

                state.last_event = err
                return StateChange(ZahirJobState.HANDLE_JOB_EXCEPTION, {
                    "message": f"Job {type(state.job).__name__} raised exception\n{err}"
                }), state

        return StateChange(ZahirJobState.ENQUEUE_JOB, {
            "message": f"Execution complete, enqueueing"
        }), state

    @classmethod
    def execute_recovery_job(cls, state) -> tuple[StateChange, None]:
        """Execute a recovery job. Similar to execute_job, but different eventing on failure/completion."""

        job_timeout = state.job.options.job_timeout if state.job.options else None

        # Emit a JobStartedEvent when we begin executing the claimed job.
        # Some job implementations don't emit this implicitly, so the
        # worker should surface it when execution begins.
        state.output_queue.put(JobStartedEvent(workflow_id=state.workflow_id, job_id=state.job.job_id))

        with ThreadPoolExecutor(max_workers=1) as ex:
            try:
                job_gen_result = ex.submit(
                    handle_job_events,
                    state.job_gen,
                    output_queue=state.output_queue,
                    state=state,
                    workflow_id=state.workflow_id,
                    job_id=cast(str, state.job.job_id),
                ).result(timeout=job_timeout)

                if isinstance(job_gen_result, JobOutputEvent):
                    # store the event for the next handler to inspect. I think this should
                    # be invariant for recover vs normal workflows.

                    state.last_event = job_gen_result
                    return StateChange(ZahirJobState.HANDLE_JOB_OUTPUT, {
                        "message": f"Recovery job {type(state.job).__name__} produced output"
                    }), state
                if isinstance(job_gen_result, Await):
                    # our job is now awaiting another; switch to that before resuming the first one
                    # recovery workflows are also allowed await, of course.
                    state.await_event = job_gen_result
                    return StateChange(ZahirJobState.HANDLE_AWAIT, {
                        "message": f"Recovery job {type(state.job).__name__} is awaiting another job"
                    }), state

                if job_gen_result is None:
                    # differs between recovery and normal workflows
                    return StateChange(ZahirJobState.HANDLE_RECOVERY_JOB_COMPLETE_NO_OUTPUT, {
                        "message": f"Recovery job {type(state.job).__name__} completed with no output"
                    }), state

            except FutureTimeoutError:
                # differs between recovery and normal workflows

                return StateChange(ZahirJobState.HANDLE_RECOVERY_JOB_TIMEOUT, {
                    "message": f"Recovery job {type(state.job).__name__} timed out"
                }), state
            except Exception as err:
                # differs between recovery and normal workflows

                state.last_event = err
                return StateChange(ZahirJobState.HANDLE_RECOVERY_JOB_EXCEPTION, {
                    "message": f"Recovery job {type(state.job).__name__} raised exception\n{err}"
                }), state

        return StateChange(ZahirJobState.ENQUEUE_JOB, {
            "message": f"Recovery execution complete, enqueueing"
        }), state


    @classmethod
    def get_state(cls, state_name: ZahirJobState):
        return getattr(cls, state_name.value)


def zahir_job_worker(context: Context, output_queue: OutputQueue, workflow_id: str) -> None:
    """Repeatly request and execute jobs from the job registry until
    there's nothing else to be done. Communicate events back to the
    supervisor process.

    """

    state = ZahirWorkerState(context, output_queue, workflow_id)
    current = StateChange(ZahirJobState.START, {
        "message": "Starting job worker"
    })

    # ...so I put a workflow engine inside your workflow engine
    while True:
        # We don't terminate this loop internally; the overseer process does based on completion events

        # run through our second secret workflow engine's steps repeatedly to update the job state
        handler = ZahirJobStateMachine.get_state(current.state)
        result = handler(state)

        if result is None:
            raise RuntimeError(f"ZahirJobStateMachine handler {handler} returned None unexpectedly")

        next_state, state = result
        current = next_state


def handle_job_events(gen, *, output_queue, state, workflow_id, job_id) -> Await | JobOutputEvent | None:
    """Sent job output items to the output queue."""

    if not isinstance(gen, Iterator):
        raise TypeError("gen must be an Iterator")

    queue: list[ZahirEvent | Job] = []
    if waiting_for_pid := state.awaiting_jobs.get(state.job.job_id):
        output = state.job_outputs.get(waiting_for_pid)
        error = state.job_errors.get(waiting_for_pid)

        if error is not None:
            event = state.job_gen.throw(error)
        else:
            event = state.job_gen.send(output)

        queue.append(event)

        # we might have further awaits, so tidy up the state so we can await yet again!
        del state.awaiting_jobs[state.job.job_id]

        if waiting_for_pid in state.job_outputs:
            del state.job_outputs[waiting_for_pid]

        if waiting_for_pid in state.job_errors:
             # The user might try-catch, so several errors can be thrown into the job
            del state.job_errors[waiting_for_pid]

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
                JobRecoveryStarted,
                JobRecoveryTimeout,
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
                output_queue.put(JobCompletedEvent(workflow_id=workflow_id, job_id=job_id, duration_seconds=0.0))

                return item

        elif isinstance(item, Job):
            # new subjob, yield as a serialised event upstream

            output_queue.put(
                JobEvent(
                    job=item.save(),
                )
            )

            continue
