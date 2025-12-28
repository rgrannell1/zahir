
from datetime import datetime, timezone
import time
import multiprocessing
from typing import cast
from zahir.base_types import Context, Job, Scope
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

type OutputQueue = multiprocessing.Queue["ZahirEvent"]


from concurrent.futures import (
    ThreadPoolExecutor,
    TimeoutError as FutureTimeoutError,
)

def zahir_job_worker(scope: Scope, output_queue: OutputQueue, workflow_id: str) -> None:
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
                    handle_job_output,
                    type(job).run(context, job.input, job.dependencies),
                    output_queue=output_queue,
                    workflow_id=workflow_id,
                    job_id=job_id,
                ).result(timeout=job_timeout)
            except FutureTimeoutError:
                output_queue.put(
                    JobTimeoutEvent(
                        workflow_id=workflow_id,
                        job_id=job_id,
                        duration_seconds=cast(float, job_timeout),
                    )
                )
                return
            output_queue.put(
                JobCompletedEvent(
                    workflow_id=workflow_id,
                    job_id=job_id,
                    duration_seconds=(
                        datetime.now(tz=timezone.utc) - start_time
                    ).total_seconds(),
                )
            )

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
                    handle_job_output,
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

def handle_job_output(gen, *, output_queue, workflow_id, job_id):
    """Sent job output items to the output queue."""

    for item in gen:
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
