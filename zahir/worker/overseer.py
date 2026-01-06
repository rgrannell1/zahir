"""Workers poll centrally for jobs to run, lease them, and return results back centrally. They report quiescence when there's nothing left to do, so the supervisor task can exit gracefully."""

from collections.abc import Iterator
import multiprocessing
import os

from zahir.base_types import JobState
from zahir.events import (
    JobCompletedEvent,
    JobIrrecoverableEvent,
    JobPausedEvent,
    JobPrecheckFailedEvent,
    JobRecoveryTimeoutEvent,
    JobStartedEvent,
    JobTimeoutEvent,
    WorkflowCompleteEvent,
    WorkflowStartedEvent,
    ZahirEvent,
    ZahirInternalErrorEvent,
)
from zahir.exception import exception_from_text_blob
from zahir.utils.id_generator import generate_id
from zahir.worker.dependency_worker import zahir_dependency_worker
from zahir.worker.job_worker import zahir_job_worker

type OutputQueue = multiprocessing.Queue["ZahirEvent"]

import logging
import sys

# Allow users to control log level via environment variable
# Default to WARNING to reduce noise, use ZAHIR_LOG_LEVEL=INFO or DEBUG for more verbosity
log_level_name = os.getenv("ZAHIR_LOG_LEVEL", "WARNING").upper()
log_level = getattr(logging, log_level_name, logging.WARNING)

logging.basicConfig(
    level=log_level,
    stream=sys.stderr,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

for h in logging.getLogger().handlers:
    h.setLevel(log_level)

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


def shutdown(processes: list[multiprocessing.Process]) -> None:
    """Terminate and join all worker processes."""

    for proc in processes:
        if proc.is_alive():
            proc.terminate()
    for proc in processes:
        proc.join(timeout=5)


def zahir_worker_overseer(start, context, worker_count: int = 4) -> Iterator[ZahirEvent]:
    """Spawn a pool of zahir_worker processes, each polling for jobs. This layer
    is responsible for collecting events from workers and yielding them to the caller."""

    workflow_id = generate_id(3)
    output_queue: OutputQueue = multiprocessing.Queue()

    output_queue.put(WorkflowStartedEvent(workflow_id=workflow_id))

    context.job_registry.init(str(os.getpid()))
    context.job_registry.delete_claims()
    if start is not None:
        context.job_registry.add(start, output_queue)

    processes = []
    dep_proc = multiprocessing.Process(
        target=zahir_dependency_worker,
        args=(context, output_queue, workflow_id),
    )
    dep_proc.start()
    processes.append(dep_proc)

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
                exc = (
                    exception_from_text_blob(event.error)
                    if event.error
                    else RuntimeError("Unknown internal error in worker")
                )
                break

            if isinstance(event, WorkflowCompleteEvent):
                yield event
                break

            yield event

    except KeyboardInterrupt:
        pass
    finally:
        shutdown(processes)

    if exc is not None:
        raise exc
