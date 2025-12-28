from collections.abc import Iterator
from concurrent.futures import (
    ThreadPoolExecutor,
    TimeoutError as FutureTimeoutError,
)
from enum import Enum
import multiprocessing
import time
from typing import cast

from zahir.base_types import Context, Job, JobState, Scope
from zahir.context.memory import MemoryContext
from zahir.events import (
    Await,
    JobCompletedEvent,
    JobEvent,
    JobIrrecoverableEvent,
    JobOutputEvent,
    JobPausedEvent,
    JobPrecheckFailedEvent,
    JobRecoveryStarted,
    JobRecoveryTimeout,
    JobStartedEvent,
    JobTimeoutEvent,
    WorkflowOutputEvent,
    ZahirCustomEvent,
    ZahirEvent,
)
from zahir.job_registry.sqlite import SQLiteJobRegistry


class ZahirJobState(str, Enum):
    """Zahir transitions through these states when executing jobs."""

    START = "start"
    WAIT_FOR_JOB = "wait_for_job"
    POP_JOB = "pop_job"
    CHECK_PRECONDITIONS = "check_preconditions"
    EXECUTE_JOB = "execute_job"
    HANDLE_AWAIT = "handle_await"
    HANDLE_JOB_OUTPUT = "handle_job_output"
    HANDLE_JOB_COMPLETE_NO_OUTPUT = "handle_job_complete_no_output"
    HANDLE_JOB_TIMEOUT = "handle_job_timeout"


type OutputQueue = multiprocessing.Queue["ZahirEvent"]


class ZahirWorkerState:
    """Mutable state held during job execution."""

    def __init__(self, context: Context, output_queue: OutputQueue, workflow_id: str):
        self.job: Job | None = None
        self.job_gen: Iterator | None = None
        self.job_err: Exception | None = None
        # each entry is (job, job_gen, awaited: bool)
        self.run_job_stack: list[tuple[Job, Iterator, bool]] = []
        self.context: Context = context
        self.output_queue: OutputQueue = output_queue
        self.workflow_id: str = workflow_id
        self.await_event: Await | None = None
        self.job_outputs: dict[str, dict] = {}
        self.awaiting_jobs: dict[str, str] = {}

        # last_event holds a value (e.g. Await or JobOutputEvent) produced by a handler
        # Dubious LLM suggestion, keeping it for now.
        self.last_event: any = None

        self.current_job_wants_output_sent: bool = False


class ZahirJobStateMachine:
    """Manages the actual state transitions of a Zahir job; run the thing,
    handle awaits, outputs, eventing, timeouts, failures, etc."""

    @classmethod
    def start(cls, state) -> tuple[ZahirJobState, None]:
        """We start. We makre sure there's something on the job stack to run."""

        if not state.run_job_stack:
            # First, let's make sure there's a generator on the stack.
            job = state.context.job_registry.claim(state.context)
            if job is None:
                return ZahirJobState.WAIT_FOR_JOB, state

            job_gen = type(job).run(state.context, job.input, job.dependencies)
            # new top-level job is not awaited by anything on the stack
            state.run_job_stack.append((job, job_gen, False))

        return ZahirJobState.POP_JOB, state

    @classmethod
    def wait_for_job(cls, state) -> tuple[ZahirJobState, None]:
        """No jobs available; for the moment let's just sleep. In future, be cleverer
        and have dependencies suggest nap-times"""
        time.sleep(1)
        return ZahirJobState.START, state

    @classmethod
    def pop_job(cls, state) -> tuple[ZahirJobState, None]:
        """We need a job; pop one off the stack"""

        if not state.job or not state.job_gen:
            job, job_gen, awaited = state.run_job_stack.pop()
            state.job, state.job_gen = job, job_gen
            state.current_job_wants_output_sent = awaited

        return ZahirJobState.CHECK_PRECONDITIONS, state

    @classmethod
    def check_preconditions(cls, state) -> tuple[ZahirJobState, None]:
        """Can we even run this job? Check the input preconditions first."""

        job_state = state.context.job_registry.get_state(state.job.job_id)

        if job_state == JobState.CLAIMED:
            # we'll avoid re-running for `Paused` jobs.
            if errors := type(state.job).precheck(state.job.input):
                # Precheck failed; job is no longer on the stack, so
                # let's report and continue.

                state.output_queue.put(
                    JobPrecheckFailedEvent(
                        workflow_id=state.workflow_id,
                        job_id=state.job.job_id,
                        errors=errors,
                    )
                )

                return ZahirJobState.START, state
        return ZahirJobState.EXECUTE_JOB, state

    @classmethod
    def handle_await(cls, state) -> tuple[ZahirJobState, None]:
        """We received an Await event. We should put out current job back on the stack,
        pause the job formally, then load the awaited job and start executing it."""

        # Pause the current job, put it back on the worker-process stack and mark it awaited.
        state.run_job_stack.append((state.job, state.job_gen, True))
        # Current job awaits new job
        state.awaiting_jobs[state.job.job_id] = state.await_event.job.job_id

        # Pause the job please
        state.output_queue.put(
            JobPausedEvent(workflow_id=state.workflow_id, job_id=state.job.job_id)
        )

        await_event = state.await_event
        assert await_event is not None

        job = await_event.job
        state.context.job_registry.add(job)
        # TO DO also writes, then Unique ID clashes
        # state.output_queue.put(
        #    JobEvent(
        #        job=job.save(),
        #    )
        # )
        state.context.job_registry.set_state(job.job_id, JobState.CLAIMED)

        state.output_queue.put(
            JobStartedEvent(workflow_id=state.workflow_id, job_id=job.job_id)
        )

        # ...then, let's instantiate the `run` generator
        job_gen = type(job).run(state.context, job.input, job.dependencies)
        state.job, state.job_gen = job, job_gen

        # this new job is not awaited yet (it's the awaited target)
        state.current_job_wants_output_sent = False

        return ZahirJobState.START, state

    @classmethod
    def handle_job_output(cls, state) -> tuple[ZahirJobState, None]:
        """We received a job output! It's emitted upstream already; just null out the job state. Persist the output to the state if awaited; we'll pop, then pass
        the output to the awaiting job"""

        if isinstance(state.last_event, JobOutputEvent):
            state.job_outputs[state.job.job_id] = state.last_event.output

        state.job_err = None
        state.job = None
        state.job_gen = None

        return ZahirJobState.START, state

    @classmethod
    def handle_job_complete_no_output(cls, state) -> tuple[ZahirJobState, None]:
        """Mark the job as complete. Emit a completion event. Null out the job. Start over."""
        # We're also done with this subjob
        state.output_queue.put(
            JobCompletedEvent(
                workflow_id=state.workflow_id,
                job_id=state.job.job_id,
                duration_seconds=0.1,
            )
        )
        state.job_err = None
        state.job = None
        state.job_gen = None

        return ZahirJobState.START, state

    @classmethod
    def handle_job_timeout(cls, state) -> tuple[ZahirJobState, None]:
        job_timeout = state.job.options.job_timeout if state.job.options else None

        state.output_queue.put(
            JobTimeoutEvent(
                workflow_id=state.workflow_id,
                job_id=state.job.job_id,
                duration_seconds=cast(float, job_timeout),
            )
        )
        state.output_queue.put(
            JobCompletedEvent(
                workflow_id=state.workflow_id,
                job_id=state.job.job_id,
                duration_seconds=0.1,
            )
        )

        # it's fine, no output to give even if awaited.
        return ZahirJobState.HANDLE_JOB_COMPLETE_NO_OUTPUT, state

    @classmethod
    def execute_job(cls, state) -> tuple[ZahirJobState, None]:
        job_timeout = state.job.options.job_timeout if state.job.options else None

        # Emit a JobStartedEvent when we begin executing the claimed job.
        # Some job implementations don't emit this implicitly, so the
        # worker should surface it when execution begins.
        state.output_queue.put(
            JobStartedEvent(workflow_id=state.workflow_id, job_id=state.job.job_id)
        )

        # TO DO send failures as `throws`!

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
                    # store the event for the next handler to inspect
                    state.last_event = job_gen_result
                    return ZahirJobState.HANDLE_JOB_OUTPUT, state
                if isinstance(job_gen_result, Await):
                    # our job is now awaiting another; switch to that before resuming the first one
                    state.await_event = job_gen_result
                    return ZahirJobState.HANDLE_AWAIT, state
                if job_gen_result is None:
                    return ZahirJobState.HANDLE_JOB_COMPLETE_NO_OUTPUT, state

            except FutureTimeoutError:
                return ZahirJobState.HANDLE_JOB_TIMEOUT, state
            except Exception as err:
                raise err
                # TODO wire in recovery mechanism (what a faff to keep, though
                # I gues it's worth it)

        return ZahirJobState.START, state

    @classmethod
    def get_state(cls, state_name: ZahirJobState):
        return getattr(cls, state_name.value)


def zahir_job_worker(scope: Scope, output_queue: OutputQueue, workflow_id: str) -> None:
    """Repeatly request and execute jobs from the job registry until
    there's nothing else to be done. Communicate events back to the
    supervisor process.

    """

    # bad, bold. dependency injection. temporary.
    job_registry = SQLiteJobRegistry("jobs.db")
    context = MemoryContext(scope=scope, job_registry=job_registry)

    state = ZahirWorkerState(context, output_queue, workflow_id)
    current = ZahirJobState.START

    # ...so I put a workflow engine inside your workflow engine
    while True:
        # We don't terminate this loop; the overseer process does based on completion events

        # run through our second secret workflow engine's steps repeatedly to update the job state
        handler = ZahirJobStateMachine.get_state(current)
        next_state, state = handler(state)
        current = next_state


def handle_job_events(
    gen, *, output_queue, state, workflow_id, job_id
) -> Await | JobOutputEvent | None:
    """Sent job output items to the output queue."""

    queue: list[ZahirEvent | Job] = []
    if waiting_for := state.awaiting_jobs.get(state.job.job_id):
        output = state.job_outputs.get(waiting_for)
        event = state.job_gen.send(output)
        queue.append(event)

        # we might have further awaits, so tidy up the state so we can await yet again!
        del state.awaiting_jobs[state.job.job_id]
        del state.job_outputs[waiting_for]

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
                JobOutputEvent,
                JobPrecheckFailedEvent,
                JobRecoveryStarted,
                JobRecoveryTimeout,
                JobStartedEvent,
                JobTimeoutEvent,
                WorkflowOutputEvent,
                ZahirCustomEvent,
                JobIrrecoverableEvent,
            ),
        ):
            item.workflow_id = workflow_id
            item.job_id = job_id

            output_queue.put(item)

            if isinstance(item, JobOutputEvent):
                # Nothing more to be done for this generator
                output_queue.put(
                    JobCompletedEvent(
                        workflow_id=workflow_id, job_id=job_id, duration_seconds=0.0
                    )
                )

                return item

        elif isinstance(item, Job):
            # new subjob, yield as a serialised event upstream

            output_queue.put(
                JobEvent(
                    job=item.save(),
                )
            )

            continue
