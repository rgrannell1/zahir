"""Workers poll centrally for jobs to run, lease them, and return results back centrally. They report quiescence when there's nothing left to do, so the supervisor task can exit gracefully.

"""


from datetime import datetime, timezone
import time
import multiprocessing
from typing import Iterator, cast
from zahir.base_types import Job, JobState, Scope, SerialisedJob
from zahir.context.memory import MemoryContext
from zahir.events import JobEvent, JobOutputEvent, JobStartedEvent, WorkflowOutputEvent, ZahirCustomEvent, ZahirEvent
from zahir.job_registry.sqlite import SQLiteJobRegistry
from zahir.utils.id_generator import generate_id

type OutputQueue = multiprocessing.Queue["ZahirEvent"]


def handle_job_output(item, output_queue: OutputQueue, workflow_id: str, job_id: str) -> None:
    """Sent job output items to the output queue."""

    if isinstance(item, JobOutputEvent):
        # this job is done & has an output

        item.workflow_id = workflow_id
        item.job_id = job_id
        output_queue.put(item)
    elif isinstance(item, WorkflowOutputEvent):
        # yield the workflow output upstream

        item.workflow_id = workflow_id
        output_queue.put(item)
    elif isinstance(item, ZahirCustomEvent):
        # something custom, yield upstream

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

def execute_job(
      job_id: str,
      job: Job,
      context: "Context",
      workflow_id: str,
      output_queue: OutputQueue) -> None:
  """Execute a single job, sending events to the output queue."""

  # TODO timeout is missing from this implementation. Run in a single-threaded worker

  try:
    output_queue.put(JobStartedEvent(workflow_id, job_id))

    for item in type(job).run(context, job.input, job.dependencies):
      handle_job_output(item, output_queue, workflow_id, job_id)

  except Exception as err:
     for item in type(job).recover(context, job.input, job.dependencies, err):
        handle_job_output(item, output_queue, workflow_id, job_id)


def zahir_worker(
      scope: Scope,
      output_queue: OutputQueue,
      workflow_id: str) -> None:
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

    submit_time = datetime.now(tz=timezone.utc)
    execute_job(
      job.job_id,
      job,
      context,
      workflow_id,
      output_queue,
    )

def load_job(context: "Context", event: JobEvent) -> Job:
    return Job.load(context, event.job)


def handle_supervisor_event(event: ZahirEvent, context: "Context", job_registry: SQLiteJobRegistry) -> None:
    """Handle events in the supervisor process"""

    if isinstance(event, JobStartedEvent):
        job_registry.set_state(event.job_id, JobState.RUNNING)
    elif isinstance(event, JobEvent):
        # register the new job
        job_registry.add(load_job(context, event))
    elif isinstance(event, JobOutputEvent):
        # store the job output
        job_registry.set_output(cast(str, event.job_id), event.output)
        job_registry.set_state(cast(str, event.job_id), JobState.COMPLETED)


def zahir_worker_pool(scope, worker_count: int = 4) -> Iterator[WorkflowOutputEvent|JobOutputEvent|ZahirCustomEvent]:
    """Spawn a pool of zahir_worker processes, each polling for jobs. This layer
    is responsible for collecting events from workers and yielding them to the caller."""

    output_queue: OutputQueue = multiprocessing.Queue()

    # bad, dependency injection
    context = MemoryContext(scope=scope)
    job_registry = SQLiteJobRegistry("jobs.db")

    workflow_id = generate_id(3)

    processes = []
    for _ in range(worker_count):
        process = multiprocessing.Process(target=zahir_worker, args=(scope, output_queue, workflow_id))
        process.start()
        processes.append(process)
    try:
        while True:
            event = output_queue.get()
            handle_supervisor_event(event, context, job_registry)
            yield event
    except KeyboardInterrupt:
        for process in processes:
            process.terminate()
        for process in processes:
            process.join()
