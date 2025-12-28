from datetime import UTC, datetime
import multiprocessing
import time
from typing import Generator, Iterator, cast
from concurrent.futures import (
    ThreadPoolExecutor,
    TimeoutError as FutureTimeoutError,
)

from zahir.base_types import Context, Job, JobState, Scope
from zahir.context.memory import MemoryContext
from zahir.events import (
    Await,
    JobCompletedEvent,
    JobEvent,
    JobIrrecoverableEvent,
    JobOutputEvent,
    JobPrecheckFailedEvent,
    JobRecoveryStarted,
    JobRecoveryTimeout,
    JobStartedEvent,
    JobTimeoutEvent,
    WorkflowOutputEvent,
    ZahirCustomEvent,
    ZahirEvent,
)
from zahir.job_registry.sqlite import SQLiteJobRegistry

type OutputQueue = multiprocessing.Queue["ZahirEvent"]


def zahir_job_worker(scope: Scope, output_queue: OutputQueue, workflow_id: str) -> None:
    """Repeatly request and execute jobs from the job registry until
    there's nothing else to be done. Communicate events back to the
    supervisor process.
    """

    # bad, dependency injection. temporary.
    job_registry = SQLiteJobRegistry("jobs.db")
    context = MemoryContext(scope=scope, job_registry=job_registry)

    run_job_stack: list[tuple[Job, Iterator]] = []
    job, job_gen = None, None
    job_output, job_err = None, None

    while True:
        if not run_job_stack:
            # First, let's make sure there's a generator on the stack.
            job = job_registry.claim(context)
            if job is None:
                time.sleep(1)
                continue

            job_gen = type(job).run(context, job.input, job.dependencies)
            run_job_stack.append((job, job_gen))

        if not job or not job_gen:
            # We need a job; pop one off the stack
            job, job_gen = run_job_stack.pop()

        if job_output:
            # TODO only send back to the workflow if the workflow we started was awaited
            # We have something to send back to the generator
            # job_gen.send(job_output.copy())
            # job_output = None
            ...
        elif job_err:
            job_gen.throw(err)
            # maybe invalidate exception

        job_state = job_registry.get_state(job.job_id)
        if job_state == JobState.CLAIMED:
            # we'll avoid re-running for Paused jobs.
            if errors := type(job).precheck(job.input):
                # Precheck failed; job is no longer on the stack, so
                # let's report and continue.

                output_queue.put(
                    JobPrecheckFailedEvent(
                        workflow_id=workflow_id,
                        job_id=job.job_id,
                        errors=errors,
                    )
                )
                continue

        job_timeout = job.options.job_timeout if job.options else None

        with ThreadPoolExecutor(max_workers=1) as ex:
            try:
                job_gen_result = ex.submit(
                    handle_job_output,
                    job_gen,
                    output_queue=output_queue,
                    workflow_id=workflow_id,
                    job_id=cast(str, job.job_id),
                ).result(timeout=job_timeout)

                # TODO: We need unusual job-states up to exceptions to throw into the original job, potentially

                # Fun! This job is waiting on another job to be done
                # TO DO put it in the job_register, we _do_ want dependencies checked in the polling job
                # but want to keep it in this process
                if isinstance(job_gen_result, Await):
                    # Pause the current job, put it back on the worker-process stack.
                    run_job_stack.append((job, job_gen))

                    # Load the job being awaited from the serialised format
                    job = Job.load(context, job_gen_result.job)

                    # ...then, let's instantiate the `run` method
                    job_gen = type(job).run(context, job.input, job.dependencies)
                    job, job_gen = None, None

                if isinstance(job_gen_result, JobOutputEvent):
                    # We're done with this subjob
                    job_output = None # TODO job_gen_result.output
                    job_err = None
                    job = None
                    job_gen_result = None
                elif job_gen_result is None:
                    # We're also done with this subjob
                    job_output = None
                    job_err = None
                    job = None
                    job_gen_result = None

            except FutureTimeoutError:
                output_queue.put(
                    JobTimeoutEvent(
                        workflow_id=workflow_id,
                        job_id=job.job_id,
                        duration_seconds=cast(float, job_timeout),
                    )
                )
                output_queue.put(
                    JobCompletedEvent(
                        workflow_id=workflow_id, job_id=job.job_id, duration_seconds=0.1
                    )
                )
            except Exception as err:
                ...
                # TODO wire in recovery mechanism (what a faff to keep it)



def handle_job_output(
    gen, *, output_queue, workflow_id, job_id
) -> Await | JobOutputEvent | None:
    """Sent job output items to the output queue."""

    # TODO: JobOutputEvent needs special handling

    for item in gen:
        if isinstance(item, Await):
            return item

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

            if isinstance(item, JobOutputEvent):
                # Nothing more to be done for this generator
                output_queue.put(
                    JobCompletedEvent(
                        workflow_id=workflow_id, job_id=job_id, duration_seconds=0.0
                    )
                )

                return item

        elif isinstance(item, Job):
            # new subjob, yield as a serialised event upstream

            output_queue.put(
                JobEvent(
                    job=item.save(),
                )
            )

    # We're finished
    return None
