"""Workers poll centrally for jobs to run, lease them, and return results back centrally. They report quiescence when there's nothing left to do, so the supervisor task can exit gracefully."""

from collections.abc import Iterator
import multiprocessing
import os

from tblib import pickling_support  # type: ignore[import-untyped]

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
    WorkflowOutputEvent,
    WorkflowStartedEvent,
    ZahirCustomEvent,
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

logging.basicConfig(
    level=logging.INFO,  # global default
    stream=sys.stderr,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

for h in logging.getLogger().handlers:
    h.setLevel(logging.INFO)


pickling_support.install()

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
    for proc in processes:
        if proc.is_alive():
            proc.terminate()
    for proc in processes:
        proc.join(timeout=5)


def zahir_worker_overseer(start, context, worker_count: int = 4, all_events: bool = False) -> Iterator[ZahirEvent]:
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

    exc: BaseException | None = None
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
                if all_events:
                    yield event
                break

            if all_events or isinstance(event, (WorkflowOutputEvent, ZahirCustomEvent)):
                yield event

    except KeyboardInterrupt:
        pass
    finally:
        shutdown(processes)

    if exc is not None:
        raise exc
