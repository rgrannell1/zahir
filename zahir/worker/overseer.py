"""The overseer dispatches jobs to workers via input queues. Workers emit when ready, and the overseer pushes jobs to ready workers fairly."""

from collections.abc import Iterator
from enum import StrEnum
import multiprocessing
import os

from zahir.base_types import Context, JobInstance, JobState
from zahir.events import (
    JobAssignedEvent,
    JobCompletedEvent,
    JobIrrecoverableEvent,
    JobPausedEvent,
    JobPrecheckFailedEvent,
    JobReadyEvent,
    JobRecoveryTimeoutEvent,
    JobStartedEvent,
    JobTimeoutEvent,
    JobWorkerWaitingEvent,
    WorkflowCompleteEvent,
    WorkflowStartedEvent,
    ZahirEvent,
    ZahirInternalErrorEvent,
)
from zahir.exception import exception_from_text_blob
from zahir.serialise import deserialise_event, serialise_event
from zahir.utils.id_generator import generate_id
from zahir.worker.dependency_worker import zahir_dependency_worker
from zahir.worker.job_worker import zahir_job_worker

type OutputQueue = multiprocessing.Queue["ZahirEvent"]
type InputQueue = multiprocessing.Queue["ZahirEvent"]


from zahir.utils.logging_config import configure_logging, get_logger

# Configure logging for this process
configure_logging()
log = get_logger(__name__)

type JobStateEvent = (
    JobStartedEvent
    | JobPrecheckFailedEvent
    | JobTimeoutEvent
    | JobRecoveryTimeoutEvent
    | JobIrrecoverableEvent
    | JobCompletedEvent
    | JobPausedEvent
)

EVENT_TO_STATE: dict[type[ZahirEvent], JobState] = {
    # What jobstate does each event correspond to?
    JobStartedEvent: JobState.RUNNING,
    JobPrecheckFailedEvent: JobState.PRECHECK_FAILED,
    JobTimeoutEvent: JobState.TIMED_OUT,
    JobRecoveryTimeoutEvent: JobState.RECOVERY_TIMED_OUT,
    JobIrrecoverableEvent: JobState.IRRECOVERABLE,
    JobCompletedEvent: JobState.COMPLETED,
    JobPausedEvent: JobState.PAUSED,
}


class WorkerState(StrEnum):
    READY = "READY"
    BUSY = "BUSY"


class WorkerPool:
    """Manage a pool of worker processes."""

    processes: list[multiprocessing.Process]
    process_queues: dict[int, InputQueue]
    process_states: dict[int, "WorkerState"]


def shutdown(processes: list[multiprocessing.Process]) -> None:
    """Terminate and join all worker processes."""

    for proc in processes:
        if proc.is_alive():
            proc.terminate()
    for proc in processes:
        proc.join(timeout=5)


def dispatch_jobs_to_workers(
    context: Context,
    process_queues: dict[int, InputQueue],
    process_states: dict[int, "WorkerState"],
    ready_worker_queue: list[int],
    workflow_id: str,
    output_queue: OutputQueue,
) -> None:
    """Dispatch READY jobs to READY workers fairly using round-robin.

    Jobs are fetched from the registry and assigned to workers in order.
    Workers are tracked in ready_worker_queue to ensure fair distribution.
    """
    job_registry = context.job_registry

    # Fetch all READY jobs (not yet dispatched)
    ready_jobs: list[JobInstance] = [job_info.job for job_info in job_registry.jobs(context, state=JobState.READY)]

    # Dispatch jobs to ready workers (round-robin via queue)
    while ready_jobs and ready_worker_queue:
        job = ready_jobs.pop(0)
        worker_pid = ready_worker_queue.pop(0)

        # Verify worker is still in READY state
        if process_states.get(worker_pid) != WorkerState.READY:
            # Worker no longer ready, put job back
            ready_jobs.insert(0, job)
            continue

        # Mark worker as BUSY before dispatching
        process_states[worker_pid] = WorkerState.BUSY

        # Set job state to RUNNING to prevent re-dispatch
        job_registry.set_state(
            context, job.job_id, job.spec.type, workflow_id, output_queue, JobState.RUNNING, recovery=False
        )

        # Send job assignment to worker via input queue
        input_queue = process_queues[worker_pid]
        input_queue.put(
            serialise_event(
                context,
                JobAssignedEvent(
                    workflow_id=workflow_id,
                    job_id=job.job_id,
                    job_type=job.spec.type,
                ),
            )
        )


def start_zahir_overseer(context: Context, start: JobInstance, worker_count: int = 4):
    """Start processes, create queues"""

    workflow_id = generate_id()
    output_queue: OutputQueue = multiprocessing.Queue()

    output_queue.put(serialise_event(context, WorkflowStartedEvent(workflow_id=workflow_id)))

    current_pid = os.getpid()
    context.job_registry.init(str(current_pid))
    context.job_registry.on_startup()

    # Start with initial job if provided
    if start is not None:
        context.job_registry.add(context, start, output_queue)

    # Start a single dependency-checker process
    processes = []
    dep_proc = multiprocessing.Process(
        target=zahir_dependency_worker,
        args=(context, output_queue, workflow_id),
    )
    dep_proc.start()
    processes.append(dep_proc)

    # Job queues for each process
    process_queues: dict[int, InputQueue] = {}

    # Start each worker process
    for _ in range(worker_count - 1):
        input_queue: InputQueue = multiprocessing.Queue()

        proc = multiprocessing.Process(
            target=zahir_job_worker,
            args=(context, input_queue, output_queue, workflow_id),
        )

        proc.start()
        processes.append(proc)

        if not proc.pid:
            raise RuntimeError("Failed to start worker process")

        process_queues[proc.pid] = input_queue

    # Workers start as READY and will signal when busy/ready
    process_states: dict[int, WorkerState] = {
        proc.pid: WorkerState.READY for proc in processes if proc.pid is not None and proc.pid in process_queues
    }

    # Ready worker queue for fair round-robin dispatch (excludes dependency worker)
    ready_worker_queue: list[int] = list(process_queues.keys())

    return processes, process_queues, process_states, ready_worker_queue, output_queue, workflow_id


def zahir_worker_overseer(start: JobInstance | None, context, worker_count: int = 4) -> Iterator[ZahirEvent]:
    """Spawn a pool of zahir_worker processes, each polling for jobs. This layer
    is responsible for collecting events from workers and yielding them to the caller."""

    processes, process_queues, process_states, ready_worker_queue, output_queue, workflow_id = start_zahir_overseer(
        context, start, worker_count
    )

    exc: Exception | None = None
    try:
        while True:
            event = deserialise_event(context, output_queue.get())

            if isinstance(event, ZahirInternalErrorEvent):
                exc = (
                    exception_from_text_blob(event.error)
                    if event.error
                    else RuntimeError("Unknown internal error in worker")
                )
                break

            elif isinstance(event, JobWorkerWaitingEvent):
                # Worker says it is ready for more work
                process_states[event.pid] = WorkerState.READY
                # Add to round-robin queue if not already present
                if event.pid not in ready_worker_queue:
                    ready_worker_queue.append(event.pid)
                # Try to dispatch any ready jobs to this worker
                dispatch_jobs_to_workers(
                    context, process_queues, process_states, ready_worker_queue, workflow_id, output_queue
                )
                # Don't yield internal worker events
                continue

            elif isinstance(event, JobReadyEvent):
                # Dependency worker detected jobs are ready - dispatch to workers
                dispatch_jobs_to_workers(
                    context, process_queues, process_states, ready_worker_queue, workflow_id, output_queue
                )
                # Don't yield internal coordination events
                continue

            elif isinstance(event, WorkflowCompleteEvent):
                yield event
                break

            yield event

    except KeyboardInterrupt:
        pass
    finally:
        shutdown(processes)
        context.job_registry.close()

    if exc is not None:
        raise exc
