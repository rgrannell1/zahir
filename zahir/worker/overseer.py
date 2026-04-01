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


class _ThroughputTracker:
    """Tracks windowed job-completion rate for the overseer."""

    WINDOW_SECONDS = 10.0

    def __init__(self) -> None:
        self._total = 0
        self._timestamps: list[float] = []

    def record(self) -> OverseerThroughputStats:
        now = time.monotonic()
        self._total += 1
        self._timestamps.append(now)

        interval_ms: float | None = None
        if len(self._timestamps) >= 2:
            interval_ms = (self._timestamps[-1] - self._timestamps[-2]) * 1000

        cutoff = now - self.WINDOW_SECONDS
        self._timestamps = [ts for ts in self._timestamps if ts > cutoff]

        return OverseerThroughputStats(
            timestamp=datetime.now(UTC),
            jobs_per_second=len(self._timestamps) / self.WINDOW_SECONDS,
            jobs_completed_total=self._total,
            completion_interval_ms=interval_ms,
        )


def shutdown(processes: list[multiprocessing.Process]) -> None:
    """Terminate and join all worker processes."""
    for proc in processes:
        if proc.is_alive():
            proc.terminate()
    for proc in processes:
        proc.join(timeout=5)


def _fetch_ready_jobs(
    context: Context,
    workflow_id: str,
    max_count: int,
) -> list[tuple[JobInstance, str]]:
    """Collect up to max_count READY jobs, current workflow first."""
    job_registry = context.job_registry
    ready_jobs: list[tuple[JobInstance, str]] = []

    for job_info in job_registry.jobs(context, state=JobState.READY, workflow_id=workflow_id, limit=max_count):
        ready_jobs.append((job_info.job, job_info.workflow_id or workflow_id))

    remaining = max_count - len(ready_jobs)
    if remaining > 0:
        for wf_id in job_registry.get_active_workflow_ids():
            if wf_id == workflow_id or remaining <= 0:
                continue
            for job_info in job_registry.jobs(context, state=JobState.READY, workflow_id=wf_id, limit=remaining):
                ready_jobs.append((job_info.job, job_info.workflow_id or wf_id))
            remaining = max_count - len(ready_jobs)

    return ready_jobs


def dispatch_jobs_to_workers(
    context: Context,
    process_queues: dict[int, InputQueue],
    process_states: dict[int, "WorkerState"],
    ready_worker_queue: list[int],
    workflow_id: str,
    output_queue: OutputQueue,
    emitter: MonitoringEmitter | None = None,
) -> None:
    """Dispatch ready jobs to idle workers using round-robin.

    Limits dispatch to the number of currently waiting workers so that no
    single worker claims an entire batch before others get a chance to signal.
    """
    dispatch_start = time.monotonic()
    max_to_dispatch = len(ready_worker_queue)

    if max_to_dispatch == 0:
        if emitter is not None:
            emitter.emit(OverseerDispatchStats(
                timestamp=datetime.now(UTC), dispatch_duration_ms=0.0, jobs_dispatched=0,
            ))
        return

    ready_jobs = _fetch_ready_jobs(context, workflow_id, max_to_dispatch)
    jobs_dispatched = 0

    while ready_jobs and ready_worker_queue and jobs_dispatched < max_to_dispatch:
        job, job_workflow_id = ready_jobs.pop(0)
        worker_pid = ready_worker_queue.pop(0)

        if process_states.get(worker_pid) != WorkerState.READY:
            ready_jobs.insert(0, (job, job_workflow_id))
            continue

        process_states[worker_pid] = WorkerState.BUSY
        context.job_registry.set_state(
            context, job.job_id, job.spec.type, job_workflow_id,
            output_queue, JobState.RUNNING, recovery=False, pid=worker_pid,
        )
        process_queues[worker_pid].put(
            serialise_event(context, JobAssignedEvent(
                workflow_id=job_workflow_id, job_id=job.job_id, job_type=job.spec.type,
            ))
        )
        jobs_dispatched += 1

    if emitter is not None:
        emitter.emit(OverseerDispatchStats(
            timestamp=datetime.now(UTC),
            dispatch_duration_ms=(time.monotonic() - dispatch_start) * 1000,
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

    context.job_registry.init(str(os.getpid()))
    context.job_registry.on_startup()

    workflow_log_dir: str | None = None
    if log_output_dir:
        workflow_log_dir = str(create_workflow_log_dir(log_output_dir, start_job_type))

    if start is not None:
        context.job_registry.add(context, start, output_queue, workflow_id)

    processes = []
    dep_proc = multiprocessing.Process(
        target=zahir_dependency_worker,
        args=(context, output_queue, workflow_id, workflow_log_dir, monitoring_emitter),
    )
    dep_proc.start()
    processes.append(dep_proc)

    process_queues: dict[int, InputQueue] = {}
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

    process_states: dict[int, WorkerState] = {
        pid: WorkerState.READY for pid in process_queues
    }
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
    """Spawn a pool of worker processes, collect their events, and yield them to the caller."""
    processes, process_queues, process_states, ready_worker_queue, output_queue, workflow_id = start_zahir_overseer(
        context, start, worker_count,
        log_output_dir=log_output_dir, start_job_type=start_job_type,
        monitoring_emitter=monitoring_emitter,
    )

    throughput = _ThroughputTracker()

    def _dispatch():
        dispatch_jobs_to_workers(
            context, process_queues, process_states, ready_worker_queue,
            workflow_id, output_queue, emitter=monitoring_emitter,
        )

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
                _dispatch()
                continue

            elif isinstance(event, JobReadyEvent):
                _dispatch()
                continue

            elif isinstance(event, WorkflowCompleteEvent):
                yield event
                break

            if isinstance(event, JobCompletedEvent):
                stats = throughput.record()
                if monitoring_emitter is not None:
                    monitoring_emitter.emit(stats)

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
