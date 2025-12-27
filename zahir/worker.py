"""Workers poll centrally for jobs to run, lease them, and return results back centrally. They report quiescence when there's nothing left to do, so the supervisor task can exit gracefully."""

from datetime import datetime, timezone
import time
import multiprocessing
from typing import Iterator, cast
from zahir.base_types import Context, Job, JobState, Scope, SerialisedJob
from zahir.context.memory import MemoryContext
from zahir.events import (
    JobCompletedEvent,
    JobEvent,
    JobOutputEvent,
    JobPrecheckFailedEvent,
    JobRecoveryStarted,
    JobRecoveryTimeout,
    JobStartedEvent,
    JobTimeoutEvent,
    WorkflowOutputEvent,
    ZahirCustomEvent,
    JobIrrecoverableEvent,
    ZahirEvent,
)
from zahir.job_registry.sqlite import SQLiteJobRegistry
from zahir.utils.id_generator import generate_id

type OutputQueue = multiprocessing.Queue["ZahirEvent"]


from datetime import datetime, timezone
from concurrent.futures import (
    ThreadPoolExecutor,
    TimeoutError as FutureTimeoutError,
)


def handle_job_output(
    item, output_queue: OutputQueue, workflow_id: str, job_id: str
) -> None:
    """Sent job output items to the output queue."""

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
            JobIrrecoverableEvent
        ),
    ):
        item.workflow_id = workflow_id
        item.job_id = job_id

        output_queue.put(item)

    elif isinstance(item, Job):
        # new subjob, yield as a serialised event upstream

        output_queue.put(
            JobEvent(
                job=item.save(),
            )
        )

    if isinstance(item, JobOutputEvent):
        # ensure jobs with output also count as completed
        output_queue.put(
            JobCompletedEvent(
                workflow_id=workflow_id,
                job_id=job_id,
                duration_seconds=0.0,  # TODO
            )
        )


def drain(gen, *, output_queue, workflow_id, job_id):
    for item in gen:
        handle_job_output(item, output_queue, workflow_id, job_id)



def execute_job(
    job_id: str,
    job: Job,
    context: Context,
    workflow_id: str,
    output_queue: OutputQueue,
) -> None:
    """Execute a single job, sending events to the output queue."""

    options = job.options
    job_timeout = options.job_timeout if options else None
    recover_timeout = options.recover_timeout if options else None

    # Started!
    output_queue.put(JobStartedEvent(workflow_id, job_id))
    start_time = datetime.now(tz=timezone.utc)

    try:
        # First, let's precheck the job

        errors = type(job).precheck(job.input)
        if errors:
            # Precheck failed; report and exit
            output_queue.put(
                JobPrecheckFailedEvent(
                    workflow_id=workflow_id,
                    job_id=job_id,
                    errors=errors,
                )
            )
            return

        with ThreadPoolExecutor(max_workers=1) as ex:
            try:
                ex.submit(
                    drain,
                    type(job).run(context, job.input, job.dependencies),
                    output_queue=output_queue,
                    workflow_id=workflow_id,
                    job_id=job_id,
                ).result(timeout=job_timeout)
            except FutureTimeoutError as err:
                output_queue.put(
                    JobTimeoutEvent(
                        workflow_id=workflow_id,
                        job_id=job_id,
                        duration_seconds=cast(float, job_timeout),
                    )
                )
                raise
            output_queue.put(
                JobCompletedEvent(
                    workflow_id=workflow_id,
                    job_id=job_id,
                    duration_seconds=(datetime.now(tz=timezone.utc) - start_time).total_seconds(),
                ))

    except Exception as err:
        # hope springs eternal!
        # let's try recover
        output_queue.put(
            JobRecoveryStarted(
                workflow_id=workflow_id,
                job_id=job_id,
            )
        )

        with ThreadPoolExecutor(max_workers=1) as ex:
            try:
                ex.submit(
                    drain,
                    type(job).recover(context, job.input, job.dependencies, err),
                    output_queue=output_queue,
                    workflow_id=workflow_id,
                    job_id=job_id,
                ).result(timeout=recover_timeout)
            except FutureTimeoutError as timeout_err:
                # Recovery timed out; raise the error!
                output_queue.put(
                    JobRecoveryTimeout(
                        workflow_id=workflow_id,
                        job_id=job_id,
                        duration_seconds=cast(float, recover_timeout),
                    )
                )
                # Irrecoverable failure
                output_queue.put(
                    JobIrrecoverableEvent(
                        workflow_id=workflow_id,
                        job_id=job_id,
                        error=timeout_err,
                    )
                )
            except Exception as recovery_err:
                # Irrecoverable failure
                output_queue.put(
                    JobIrrecoverableEvent(
                        workflow_id=workflow_id,
                        job_id=job_id,
                        error=recovery_err,
                    )
                )

        end_time = datetime.now(tz=timezone.utc)

        # Finished! Let's share a timing
        output_queue.put(
            JobCompletedEvent(
                workflow_id=workflow_id,
                job_id=job_id,
                duration_seconds=(end_time - start_time).total_seconds(),
            )
        )


def zahir_worker(scope: Scope, output_queue: OutputQueue, workflow_id: str) -> None:
    """Repeatly request and execute jobs from the job registry until
    there's nothing else to be done. Communicate events back to the
    supervisor process.

    """

    # bad, dependency injection. temporary.
    job_registry = SQLiteJobRegistry("jobs.db")
    context = MemoryContext(scope=scope, job_registry=job_registry)

    while True:
        # try to claim a job
        job = job_registry.claim(context)

        if job is None:
            # TODO recentralise this timeout
            time.sleep(1)
            continue

        execute_job(
            job.job_id,
            job,
            context,
            workflow_id,
            output_queue,
        )


def load_job(context: Context, event: JobEvent) -> Job:
    return Job.load(context, event.job)

# Job-state events (TODO, via inheritance)
type JobStateEvent = (
  JobStartedEvent |
  JobPrecheckFailedEvent |
  JobTimeoutEvent |
  JobRecoveryTimeout |
  JobIrrecoverableEvent |
  JobCompletedEvent
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
        job_registry.add(load_job(context, event))

    elif isinstance(event, JobOutputEvent):
        # store the job output
        job_registry.set_output(cast(str, event.job_id), event.output)
        job_registry.set_state(cast(str, event.job_id), JobState.COMPLETED)


def zahir_worker_pool(
    context, worker_count: int = 4
) -> Iterator[WorkflowOutputEvent | JobOutputEvent | ZahirCustomEvent]:
    """Spawn a pool of zahir_worker processes, each polling for jobs. This layer
    is responsible for collecting events from workers and yielding them to the caller."""

    workflow_id = generate_id(3)
    output_queue: OutputQueue = multiprocessing.Queue()

    # TODO: reintroduce dependency evaluation loop; I think this refactor breaks dependencies
    # CRITICAL! Should be done in a separate worker.

    processes = []
    for _ in range(worker_count):
        process = multiprocessing.Process(
            target=zahir_worker, args=(context.scope, output_queue, workflow_id)
        )
        process.start()
        processes.append(process)
    try:
        while True:
            event = output_queue.get()
            handle_supervisor_event(event, context, context.job_registry)
            yield event
    except KeyboardInterrupt:
        for process in processes:
            process.terminate()
        for process in processes:
            process.join()
