"""Workers poll centrally for jobs to run, lease them, and return results back centrally. They report quiescence when there's nothing left to do, so the supervisor task can exit gracefully."""

from collections.abc import Iterator
import multiprocessing
from typing import cast

from zahir.base_types import Context, Job, JobState
from zahir.events import (
    JobCompletedEvent,
    JobEvent,
    JobIrrecoverableEvent,
    JobOutputEvent,
    JobPausedEvent,
    JobPrecheckFailedEvent,
    JobRecoveryTimeout,
    JobStartedEvent,
    JobTimeoutEvent,
    WorkflowCompleteEvent,
    WorkflowOutputEvent,
    WorkflowStartedEvent,
    ZahirCustomEvent,
    ZahirEvent,
    ZahirInternalErrorEvent,
)
from zahir.utils.id_generator import generate_id
from zahir.worker.dependency_worker import zahir_dependency_worker
from zahir.worker.job_worker import zahir_job_worker

type OutputQueue = multiprocessing.Queue["ZahirEvent"]

type JobStateEvent = (
    JobStartedEvent
    | JobPrecheckFailedEvent
    | JobTimeoutEvent
    | JobRecoveryTimeout
    | JobIrrecoverableEvent
    | JobCompletedEvent
    | JobPausedEvent
)

EVENT_TO_STATE: dict[type[ZahirEvent], JobState] = {
    # What jobstate does each event correspond to?
    JobStartedEvent: JobState.RUNNING,
    JobPrecheckFailedEvent: JobState.PRECHECK_FAILED,
    JobTimeoutEvent: JobState.TIMED_OUT,
    JobRecoveryTimeout: JobState.RECOVERY_TIMED_OUT,
    JobIrrecoverableEvent: JobState.IRRECOVERABLE,
    JobCompletedEvent: JobState.COMPLETED,
    JobPausedEvent: JobState.PAUSED,
}


def handle_supervisor_event(event: ZahirEvent, context: "Context") -> None:
    """Handle events in the supervisor process

    @param event: The event to handle
    @param context: The context
    """

    if type(event) in EVENT_TO_STATE:
        # event is a JobStateEvent, so job_id is present
        job_state_event = cast(JobStateEvent, event)
        context.job_registry.set_state(cast(str, job_state_event.job_id), EVENT_TO_STATE[type(job_state_event)])

    elif isinstance(event, JobEvent):
        # register the new job
        context.job_registry.add(Job.load(context, event.job))

    elif isinstance(event, JobOutputEvent):
        # store the job output
        context.job_registry.set_output(cast(str, event.job_id), event.output)
        context.job_registry.set_state(cast(str, event.job_id), JobState.COMPLETED)

def zahir_worker_overseer(context, worker_count: int = 4, all_events: bool = False) -> Iterator[ZahirEvent]:
    """Spawn a pool of zahir_worker processes, each polling for jobs. This layer
    is responsible for collecting events from workers and yielding them to the caller."""

    workflow_id = generate_id(3)
    output_queue: OutputQueue = multiprocessing.Queue()

    processes = []
    dep_proc = multiprocessing.Process(
        target=zahir_dependency_worker,
        args=(context, output_queue, workflow_id),
    )
    dep_proc.start()
    processes.append(dep_proc)

    output_queue.put(WorkflowStartedEvent(workflow_id=workflow_id))

    for _ in range(worker_count - 1):
        proc = multiprocessing.Process(
            target=zahir_job_worker,
            args=(context, output_queue, workflow_id),
        )
        proc.start()
        processes.append(proc)

    exc: Exception | None = None
    try:
        while True:
            event = output_queue.get()

            if isinstance(event, ZahirInternalErrorEvent):
                exc = event.error.to_exception() if event.error else RuntimeError("Unknown internal error in worker")
                break

            if isinstance(event, WorkflowCompleteEvent):
                if all_events:
                    yield event
                break

            handle_supervisor_event(event, context)
            if all_events or isinstance(event, (WorkflowOutputEvent, ZahirCustomEvent)):
                yield event

    except KeyboardInterrupt:
        pass
    finally:
        for proc in processes:
            if proc.is_alive():
                proc.terminate()
        for proc in processes:
            proc.join(timeout=5)

    if exc is not None:
        raise exc
