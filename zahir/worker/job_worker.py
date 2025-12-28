from datetime import UTC, datetime
from enum import Enum
import multiprocessing
import time
from typing import Generator, Iterator, Tuple, cast
from concurrent.futures import (
    ThreadPoolExecutor,
    TimeoutError as FutureTimeoutError,
)

from zahir.base_types import Context, Job, JobState, Scope
from zahir.context.memory import MemoryContext
from zahir.events import (
    Await,
    JobCompletedEvent,
    JobEvent,
    JobIrrecoverableEvent,
    JobOutputEvent,
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

type OutputQueue = multiprocessing.Queue["ZahirEvent"]

class ZahirWorkerState:
    def __init__(self, context: Context, output_queue: OutputQueue, workflow_id: str):
        self.job: Job | None = None
        self.job_gen: Iterator | None = None
        self.job_output: any = None
        self.job_err: Exception | None = None
        self.run_job_stack: list[tuple[Job, Iterator]] = []
        self.context: Context = context
        self.output_queue: OutputQueue = output_queue
        self.workflow_id: str = workflow_id
        # last_event holds a value (e.g. Await or JobOutputEvent) produced by a handler
        self.last_event: any = None

class ZahirJobState(str, Enum):
    START = "start"
    WAIT_FOR_JOB = "wait_for_job"
    POP_JOB = "pop_job"
    CHECK_PRECONDITIONS = "check_preconditions"
    EXECUTE_JOB = "execute_job"
    HANDLE_AWAIT = "handle_await"
    HANDLE_JOB_OUTPUT = "handle_job_output"
    HANDLE_JOB_COMPLETE = "handle_job_complete"
    HANDLE_JOB_TIMEOUT = "handle_job_timeout"

class ZahirJobStateMachine:
    @classmethod
    def start(cls, state) -> Tuple[ZahirJobState, None]:
        if not state.run_job_stack:
            # First, let's make sure there's a generator on the stack.
            job = state.context.job_registry.claim(state.context)
            if job is None:
                return ZahirJobState.WAIT_FOR_JOB, state

            job_gen = type(job).run(state.context, job.input, job.dependencies)
            state.run_job_stack.append((job, job_gen))

        return ZahirJobState.POP_JOB, state

    @classmethod
    def wait_for_job(cls, state) -> Tuple[ZahirJobState, None]:
        time.sleep(1)
        return ZahirJobState.START, state

    @classmethod
    def pop_job(cls, state) -> Tuple[ZahirJobState, None]:
        if not state.job or not state.job_gen:
            # We need a job; pop one off the stack
            state.job, state.job_gen = state.run_job_stack.pop()

        return ZahirJobState.CHECK_PRECONDITIONS, state

    @classmethod
    def check_preconditions(cls, state) -> Tuple[ZahirJobState, None]:
        job_state = state.context.job_registry.get_state(state.job.job_id)
        if job_state == JobState.CLAIMED:
            # we'll avoid re-running for Paused jobs.
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
    def handle_await(cls, state, await_event: Await) -> Tuple[ZahirJobState, None]:
        # Pause the current job, put it back on the worker-process stack.
        state.run_job_stack.append((state.job, state.job_gen))

        # Load the job being awaited from the serialised format
        job = Job.load(state.context, await_event.job)

        # ...then, let's instantiate the `run` method
        job_gen = type(job).run(state.context, job.input, job.dependencies)
        state.job, state.job_gen = job, job_gen

        return ZahirJobState.START, state

    @classmethod
    def handle_job_output(cls, state) -> Tuple[ZahirJobState, None]:
        # We're done with this subjob
        state.job_output = None # TODO job_output_event.output
        state.job_err = None
        state.job = None
        state.job_gen = None

        return ZahirJobState.START, state

    @classmethod
    def handle_job_complete(cls, state) -> Tuple[ZahirJobState, None]:
        # We're also done with this subjob
        state.job_output = None
        state.job_err = None
        state.job = None
        state.job_gen = None

        return ZahirJobState.START, state

    @classmethod
    def handle_job_timeout(cls, state) -> Tuple[ZahirJobState, None]:
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
                workflow_id=state.workflow_id, job_id=state.job.job_id, duration_seconds=0.1
            )
        )

        return ZahirJobState.HANDLE_JOB_COMPLETE, state

    @classmethod
    def execute_job(cls, state) -> Tuple[ZahirJobState, None]:
        job_timeout = state.job.options.job_timeout if state.job.options else None

        with ThreadPoolExecutor(max_workers=1) as ex:
            try:
                job_gen_result = ex.submit(
                    handle_job_events,
                    state.job_gen,
                    output_queue=state.output_queue,
                    workflow_id=state.workflow_id,
                    job_id=cast(str, state.job.job_id),
                ).result(timeout=job_timeout)

                # stash the event on the state so the next handler can access it
                state.last_event = job_gen_result

                if isinstance(job_gen_result, Await):
                    return ZahirJobState.HANDLE_AWAIT, state
                if isinstance(job_gen_result, JobOutputEvent):
                    return ZahirJobState.HANDLE_JOB_OUTPUT, state
                elif job_gen_result is None:
                    return ZahirJobState.HANDLE_JOB_COMPLETE, state

            except FutureTimeoutError:
                return ZahirJobState.HANDLE_JOB_TIMEOUT, state
            except Exception as err:
                ...
                # TODO wire in recovery mechanism (what a faff to keep it)

        return ZahirJobState.START, state


    @classmethod
    def get_state(cls, state_name: ZahirJobState):
        return getattr(cls, state_name.value)

def zahir_job_worker(scope: Scope, output_queue: OutputQueue, workflow_id: str) -> None:
    """Repeatly request and execute jobs from the job registry until
    there's nothing else to be done. Communicate events back to the
    supervisor process.
    """

    # bad, dependency injection. temporary.
    job_registry = SQLiteJobRegistry("jobs.db")
    context = MemoryContext(scope=scope, job_registry=job_registry)

    state = ZahirWorkerState(context, output_queue, workflow_id)
    current = ZahirJobState.START

    # ...so I put a workflow engine inside your workflow engine
    while True:
        handler = ZahirJobStateMachine.get_state(current)
        next_state, state = handler(state)
        state.last_event = None
        current = next_state

def handle_job_events(
    gen, *, output_queue, workflow_id, job_id
) -> Await | JobOutputEvent | None:
    """Sent job output items to the output queue."""

    # TODO: JobOutputEvent needs special handling

    for item in gen:
        print(item)
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

    # We're finished
    return None
