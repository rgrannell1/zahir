from collections.abc import Iterator, Mapping
import multiprocessing
from typing import Any, cast

from zahir.base_types import Job, JobRegistry
from zahir.events import Await, JobOutputEvent, ZahirEvent

type OutputQueue = multiprocessing.Queue["ZahirEvent | Job"]


def read_job_events(
    gen: Iterator,
    *,
    job_registry: JobRegistry,
    output_queue: OutputQueue,
    state,
    workflow_id: str,
    job_id: str,
) -> Await | JobOutputEvent | None:
    """Sent job output items to the output queue."""

    if not isinstance(gen, Iterator):
        raise TypeError(f"Non-iterator passed to read_job_events for job-id: {job_id}, type: {type(gen)}")

    queue: list[ZahirEvent | Job] = []

    assert state.frame is not None

    required_pids = list(state.frame.required_jobs)

    if required_pids or state.frame.await_many:
        # Get the errors and outputs for the pid on which we were awaiting...

        # Not all jobs have to produce an output.
        outputs: list[Mapping | None] = []

        # Errors are 1:{0,1,...} per job
        errors: list[BaseException] = []

        # Collate the information for each job
        for required_pid in required_pids:
            outputs.append(job_registry.get_output(required_pid))
            job_errors = job_registry.get_errors(required_pid)
            errors += job_errors

        # So, this sucks. I ported from a custom serialiser to tblib. Turns out tblib
        # can't handle throwing into generators. We'll need to port back to a custom serialiser (screw pickle anyway)
        # to get our error-types and traces back.

        throwable_errors = []
        for error in errors:
            throwable_errors.append(Exception(str(error)))

        if errors:
            # Try to throw one exception, but let's not bury information either if there's multiple.
            throwable = (
                throwable_errors[0]
                if len(throwable_errors) == 1
                # we might want a custom ExceptionGroup type
                else ExceptionGroup("Multiple errors", throwable_errors)
            )

            event = state.frame.job_generator.throw(throwable)
        else:
            generator_input = None
            generator_input = outputs[0] if len(required_pids) == 1 else outputs
            event = state.frame.job_generator.send(generator_input)

        # something happens afterwards, so enqueue it and proceed
        queue.append(event)

        # we might have further awaits, so tidy up the state so we can await yet again!
        state.frame.required_jobs = set()

    while True:
        # deal with the `send` or `throw` event, if we have one
        item = queue.pop(0) if queue else None

        if item is None:
            try:
                item = next(gen)
            except StopIteration:
                # We're finished!
                return None

        if isinstance(item, Await):
            return item

        if isinstance(item, ZahirEvent):
            item.set_ids(workflow_id=workflow_id, job_id=job_id)

        output_queue.put(item)

        if isinstance(item, JobOutputEvent):
            # Nothing more to be done for this generator
            return item

        if isinstance(item, Job):
            # new subjob, yield as a serialised event upstream
            job_registry.add(item, output_queue)
            continue
