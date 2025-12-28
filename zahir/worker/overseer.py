"""Workers poll centrally for jobs to run, lease them, and return results back centrally. They report quiescence when there's nothing left to do, so the supervisor task can exit gracefully."""

import multiprocessing
from typing import Iterator, cast
from zahir.base_types import Context, Job, JobState
from zahir.events import (
    JobCompletedEvent,
    JobEvent,
    JobOutputEvent,
    JobPrecheckFailedEvent,
    JobRecoveryTimeout,
    JobStartedEvent,
    JobTimeoutEvent,
    WorkflowCompleteEvent,
    WorkflowOutputEvent,
    ZahirCustomEvent,
    JobIrrecoverableEvent,
    ZahirEvent,
)
from zahir.job_registry.sqlite import SQLiteJobRegistry
from zahir.utils.id_generator import generate_id
from zahir.worker.dependency_worker import zahir_dependency_worker
from zahir.worker.job_worker import zahir_job_worker

type OutputQueue = multiprocessing.Queue["ZahirEvent"]


from concurrent.futures import (
    ThreadPoolExecutor,
    TimeoutError as FutureTimeoutError,
)

# Job-state events (TODO, via inheritance)
type JobStateEvent = (
    JobStartedEvent
    | JobPrecheckFailedEvent
    | JobTimeoutEvent
    | JobRecoveryTimeout
    | JobIrrecoverableEvent
    | JobCompletedEvent
)

EVENT_TO_STATE: dict[type[ZahirEvent], JobState] = {
    # What jobstate does each event correspond to?
    JobStartedEvent: JobState.RUNNING,
    JobPrecheckFailedEvent: JobState.PRECHECK_FAILED,
    JobTimeoutEvent: JobState.TIMED_OUT,
    JobRecoveryTimeout: JobState.RECOVERY_TIMED_OUT,
    JobIrrecoverableEvent: JobState.IRRECOVERABLE,
    JobCompletedEvent: JobState.COMPLETED,
}


def handle_supervisor_event(
    event: ZahirEvent, context: "Context", job_registry: SQLiteJobRegistry
) -> None:
    """Handle events in the supervisor process"""

    if type(event) in EVENT_TO_STATE:
        # event is a JobStateEvent, so job_id is present
        job_state_event = cast(JobStateEvent, event)
        job_registry.set_state(
            cast(str, job_state_event.job_id), EVENT_TO_STATE[type(job_state_event)]
        )

    elif isinstance(event, JobEvent):
        # register the new job
        job_registry.add(Job.load(context, event.job))

    elif isinstance(event, JobOutputEvent):
        # store the job output
        job_registry.set_output(cast(str, event.job_id), event.output)
        job_registry.set_state(cast(str, event.job_id), JobState.COMPLETED)




def zahir_worker_overseer(
    context, worker_count: int = 4
) -> Iterator[WorkflowOutputEvent | JobOutputEvent | ZahirCustomEvent]:
    """Spawn a pool of zahir_worker processes, each polling for jobs. This layer
    is responsible for collecting events from workers and yielding them to the caller."""

    workflow_id = generate_id(3)
    output_queue: OutputQueue = multiprocessing.Queue()

    # TODO: reintroduce dependency evaluation loop; I think this refactor breaks dependencies
    # CRITICAL! Should be done in a separate worker.

    processes = []

    processs = multiprocessing.Process(
        target=zahir_dependency_worker, args=(context.scope, output_queue, workflow_id)
    )
    processs.start()
    processes.append(processs)

    for _ in range(worker_count - 1):
        process = multiprocessing.Process(
            target=zahir_job_worker, args=(context.scope, output_queue, workflow_id)
        )
        process.start()
        processes.append(process)
    try:
        while True:
            event = output_queue.get()

            if isinstance(event, WorkflowCompleteEvent):
                break

            handle_supervisor_event(event, context, context.job_registry)
            if isinstance(
                event, (WorkflowOutputEvent, JobOutputEvent, ZahirCustomEvent)):
                yield event
    except KeyboardInterrupt:
        pass
    finally:
        for process in processes:
            process.terminate()
        for process in processes:
            process.join()
