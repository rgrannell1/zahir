"""The overseer dispatches jobs to workers via input queues. Workers emit when ready, and the overseer pushes jobs to ready workers fairly."""

from collections.abc import Iterator
from datetime import UTC, datetime
from enum import StrEnum
import multiprocessing
from multiprocessing.queues import Queue
import os
import time

from zahir.base_types import Context, JobInstance, JobState
from zahir.events import (
    JobAssignedEvent,
    JobCompletedEvent,
    JobReadyEvent,
    JobWorkerWaitingEvent,
    WorkflowCompleteEvent,
    WorkflowStartedEvent,
    ZahirEvent,
    ZahirInternalErrorEvent,
)
from zahir.exception import exception_from_text_blob
from zahir.monitoring.emitter import MonitoringEmitter
from zahir.monitoring.events import OverseerDispatchStats, OverseerThroughputStats
from zahir.serialise import SerialisedEvent, deserialise_event, serialise_event
from zahir.utils.id_generator import generate_id
from zahir.utils.output_logging import create_workflow_log_dir
from zahir.worker.dependency_worker import zahir_dependency_worker
from zahir.worker.job_worker import zahir_job_worker
from zahir.worker.monitoring_bridge import enrich_and_emit

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
    emitter: MonitoringEmitter | None = None,
) -> None:
    """Dispatch jobs to workers fairly using round-robin.

    Jobs are fetched from the registry and assigned to workers in order.
    Workers are tracked in ready_worker_queue to ensure fair distribution.

    To prevent one worker from claiming all jobs when a large batch becomes ready,
    we limit dispatch per call. This allows other workers time to signal READY and get jobs too.
    """

    dispatch_start = time.monotonic()
    job_registry = context.job_registry

    # Only fetch as many READY jobs as we have workers to send them to.
    max_jobs_to_dispatch = len(ready_worker_queue) if ready_worker_queue else 0
    if max_jobs_to_dispatch == 0:
        if emitter is not None:
            emitter.emit(OverseerDispatchStats(
                timestamp=datetime.now(UTC),
                dispatch_duration_ms=0.0,
                jobs_dispatched=0,
            ))
        return

    remaining = max_jobs_to_dispatch
    ready_jobs: list[tuple[JobInstance, str]] = []  # (job, workflow_id)

    # Current workflow first, then backfill from older active workflows.
    for job_info in job_registry.jobs(context, state=JobState.READY, workflow_id=workflow_id, limit=remaining):
        ready_jobs.append((job_info.job, job_info.workflow_id or workflow_id))

    remaining = max_jobs_to_dispatch - len(ready_jobs)
    if remaining > 0:
        active_workflow_ids = job_registry.get_active_workflow_ids()
        for wf_id in active_workflow_ids:
            if wf_id == workflow_id or remaining <= 0:
                continue
            for job_info in job_registry.jobs(context, state=JobState.READY, workflow_id=wf_id, limit=remaining):
                ready_jobs.append((job_info.job, job_info.workflow_id or wf_id))
            remaining = max_jobs_to_dispatch - len(ready_jobs)
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

    if emitter is not None:
        dispatch_elapsed_ms = (time.monotonic() - dispatch_start) * 1000
        emitter.emit(OverseerDispatchStats(
            timestamp=datetime.now(UTC),
            dispatch_duration_ms=dispatch_elapsed_ms,
            jobs_dispatched=jobs_dispatched,
        ))


def start_zahir_overseer(
    context: Context,
    start: JobInstance | None,
    worker_count: int = 4,
    log_output_dir: str | None = None,
    start_job_type: str | None = None,
    monitoring_emitter: MonitoringEmitter | None = None,
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
        args=(context, output_queue, workflow_id, workflow_log_dir, monitoring_emitter),
    )
    dep_proc.start()
    processes.append(dep_proc)

    # Job queues for each process
    process_queues: dict[int, InputQueue] = {}

    # Start each worker process
    for idx in range(worker_count - 1):
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
    monitoring_emitter: MonitoringEmitter | None = None,
    log_output_dir: str | None = None,
    start_job_type: str | None = None,
) -> Iterator[ZahirEvent]:
    """Spawn a pool of zahir_worker processes, each polling for jobs. This layer
    is responsible for collecting events from workers and yielding them to the caller."""

    processes, process_queues, process_states, ready_worker_queue, output_queue, workflow_id = start_zahir_overseer(
        context, start, worker_count, log_output_dir=log_output_dir, start_job_type=start_job_type,
        monitoring_emitter=monitoring_emitter,
    )

    jobs_completed_total = 0
    completion_timestamps: list[float] = []
    RATE_WINDOW_SECONDS = 10.0

    exc: Exception | None = None
    try:
        while True:
            event = deserialise_event(context, output_queue.get())

            if monitoring_emitter is not None:
                enrich_and_emit(event, context, monitoring_emitter)

            if isinstance(event, ZahirInternalErrorEvent):
                exc = (
                    exception_from_text_blob(event.error)
                    if event.error
                    else RuntimeError("Unknown internal error in worker")
                )
                break

            elif isinstance(event, JobWorkerWaitingEvent):
                process_states[event.pid] = WorkerState.READY
                if event.pid not in ready_worker_queue:
                    ready_worker_queue.append(event.pid)

                dispatch_jobs_to_workers(
                    context, process_queues, process_states, ready_worker_queue, workflow_id, output_queue,
                    emitter=monitoring_emitter,
                )
                continue

            elif isinstance(event, JobReadyEvent):
                dispatch_jobs_to_workers(
                    context, process_queues, process_states, ready_worker_queue, workflow_id, output_queue,
                    emitter=monitoring_emitter,
                )
                continue

            elif isinstance(event, WorkflowCompleteEvent):
                yield event
                break

            if isinstance(event, JobCompletedEvent):
                jobs_completed_total += 1
                now = time.monotonic()
                completion_timestamps.append(now)

                if monitoring_emitter is not None:
                    completion_interval_ms: float | None = None
                    if len(completion_timestamps) >= 2:
                        completion_interval_ms = (completion_timestamps[-1] - completion_timestamps[-2]) * 1000

                    cutoff = now - RATE_WINDOW_SECONDS
                    completion_timestamps = [ts for ts in completion_timestamps if ts > cutoff]

                    monitoring_emitter.emit(OverseerThroughputStats(
                        timestamp=datetime.now(UTC),
                        jobs_per_second=len(completion_timestamps) / RATE_WINDOW_SECONDS,
                        jobs_completed_total=jobs_completed_total,
                        completion_interval_ms=completion_interval_ms,
                    ))
                else:
                    cutoff = now - RATE_WINDOW_SECONDS
                    completion_timestamps = [ts for ts in completion_timestamps if ts > cutoff]

            yield event

    except KeyboardInterrupt:
        pass
    finally:
        shutdown(processes)
        context.job_registry.close()

    if exc is not None:
        tb = getattr(exc, "__serialized_traceback__", None)
        if isinstance(tb, str) and tb.strip():
            exc.add_note("Traceback from worker process:\n" + tb.rstrip())
        raise exc
