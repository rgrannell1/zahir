"""The overseer dispatches jobs to workers via input queues. Workers emit when ready, and the overseer pushes jobs to ready workers fairly."""

from collections.abc import Iterator
from enum import StrEnum
import multiprocessing
from multiprocessing.queues import Queue
import os

from zahir.base_types import Context, JobInstance, JobState
from zahir.events import (
    JobAssignedEvent,
    JobReadyEvent,
    JobWorkerWaitingEvent,
    WorkflowCompleteEvent,
    WorkflowStartedEvent,
    ZahirEvent,
    ZahirInternalErrorEvent,
)
from zahir.exception import exception_from_text_blob
from zahir.serialise import SerialisedEvent, deserialise_event, serialise_event
from zahir.utils.id_generator import generate_id
from zahir.utils.opentelemetry import TraceContextManager
from zahir.utils.output_logging import create_workflow_log_dir
from zahir.worker.dependency_worker import zahir_dependency_worker
from zahir.worker.job_worker import zahir_job_worker

type OutputQueue = Queue[SerialisedEvent]
type InputQueue = Queue[SerialisedEvent]


from zahir.utils.logging_config import configure_logging, get_logger

configure_logging()
log = get_logger(__name__)


class WorkerState(StrEnum):
    READY = "READY"
    BUSY = "BUSY"


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
    """Dispatch jobs to workers fairly using round-robin.

    Jobs are fetched from the registry and assigned to workers in order.
    Workers are tracked in ready_worker_queue to ensure fair distribution.

    To prevent one worker from claiming all jobs when a large batch becomes ready,
    we limit dispatch per call. This allows other workers time to signal READY and get jobs too.
    """

    job_registry = context.job_registry

    # Fetch all READY jobs from all active workflows (prioritize current workflow)
    # This allows us to "backfill" older workflows while processing the current one
    ready_jobs: list[tuple[JobInstance, str]] = []  # (job, workflow_id)

    # First, get jobs from current workflow...
    for job_info in job_registry.jobs(context, state=JobState.READY, workflow_id=workflow_id):
        ready_jobs.append((job_info.job, job_info.workflow_id or workflow_id))

    # Then, get jobs from other active workflows...
    active_workflow_ids = job_registry.get_active_workflow_ids()
    for wf_id in active_workflow_ids:
        if wf_id != workflow_id:
            for job_info in job_registry.jobs(context, state=JobState.READY, workflow_id=wf_id):
                ready_jobs.append((job_info.job, job_info.workflow_id or wf_id))

    # Limit dispatch to prevent one worker from claiming all jobs in a large batch
    # do not overdispatch jobs; give each worker a fair chance
    max_jobs_to_dispatch = len(ready_worker_queue) if ready_worker_queue else 0
    jobs_dispatched = 0

    # Dispatch jobs to ready workers (round-robin via queue)
    while ready_jobs and ready_worker_queue and jobs_dispatched < max_jobs_to_dispatch:
        job, job_workflow_id = ready_jobs.pop(0)
        worker_pid = ready_worker_queue.pop(0)

        # Verify worker is still in READY state. If not, reenque
        if process_states.get(worker_pid) != WorkerState.READY:
            ready_jobs.insert(0, (job, job_workflow_id))
            continue

        # Mark worker as BUSY before dispatching
        process_states[worker_pid] = WorkerState.BUSY

        # Set job state to RUNNING to prevent re-dispatch
        # Pass the worker PID so JobStartedEvent has the correct PID
        # Use the job's workflow_id, not the current workflow_id
        job_registry.set_state(
            context,
            job.job_id,
            job.spec.type,
            job_workflow_id,
            output_queue,
            JobState.RUNNING,
            recovery=False,
            pid=worker_pid,
        )

        # Send job assignment to worker via input queue
        # Use the job's workflow_id, not the current workflow_id
        input_queue = process_queues[worker_pid]
        input_queue.put(
            serialise_event(
                context,
                JobAssignedEvent(
                    workflow_id=job_workflow_id,
                    job_id=job.job_id,
                    job_type=job.spec.type,
                ),
            )
        )
        jobs_dispatched += 1


def start_zahir_overseer(
    context: Context,
    start: JobInstance | None,
    worker_count: int = 4,
    log_output_dir: str | None = None,
    start_job_type: str | None = None,
):
    """Start processes, create queues."""

    workflow_id = generate_id()
    output_queue: OutputQueue = multiprocessing.Queue()

    output_queue.put(serialise_event(context, WorkflowStartedEvent(workflow_id=workflow_id)))

    current_pid = os.getpid()
    context.job_registry.init(str(current_pid))
    context.job_registry.on_startup()

    # Create a single shared log directory for the entire workflow run
    workflow_log_dir: str | None = None
    if log_output_dir:
        workflow_log_dir = str(create_workflow_log_dir(log_output_dir, start_job_type))

    # Start with initial job if provided
    if start is not None:
        context.job_registry.add(context, start, output_queue, workflow_id)

    # Start a single dependency-checker process for the moment
    processes = []
    dep_proc = multiprocessing.Process(
        target=zahir_dependency_worker,
        args=(context, output_queue, workflow_id, workflow_log_dir),
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
            args=(context, input_queue, output_queue, workflow_id, workflow_log_dir),
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


def zahir_worker_overseer(
    start: JobInstance | None,
    context,
    worker_count: int = 4,
    otel_output_dir: str | None = None,
    log_output_dir: str | None = None,
    start_job_type: str | None = None,
) -> Iterator[ZahirEvent]:
    """Spawn a pool of zahir_worker processes, each polling for jobs. This layer
    is responsible for collecting events from workers and yielding them to the caller."""

    processes, process_queues, process_states, ready_worker_queue, output_queue, workflow_id = start_zahir_overseer(
        context, start, worker_count, log_output_dir=log_output_dir, start_job_type=start_job_type
    )

    # Initialize open telemetry if requested
    trace_manager = None
    if otel_output_dir is not None:
        trace_manager = TraceContextManager(otel_output_dir, context)

    exc: Exception | None = None
    try:
        while True:
            event = deserialise_event(context, output_queue.get())

            # Handle trace context if enabled
            if trace_manager:
                trace_manager.handle_event(event)

            if isinstance(event, ZahirInternalErrorEvent):
                # Thing broke, lets stop and raise the error
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
                # Dispatch any ready jobs to the workers

                dispatch_jobs_to_workers(
                    context, process_queues, process_states, ready_worker_queue, workflow_id, output_queue
                )

                # Don't yield internal worker events
                continue

            elif isinstance(event, JobReadyEvent):
                # Dependency worker detected jobs are ready. Dispatch to workers
                # Don't yield internal coordination events
                dispatch_jobs_to_workers(
                    context, process_queues, process_states, ready_worker_queue, workflow_id, output_queue
                )
                continue

            elif isinstance(event, WorkflowCompleteEvent):
                # Workflow is complete, lets yield the event and break
                yield event
                break

            yield event

    except KeyboardInterrupt:
        pass
    finally:
        if trace_manager:
            trace_manager.close()
        shutdown(processes)
        context.job_registry.close()

    if exc is not None:
        raise exc
