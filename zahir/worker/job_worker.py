import atexit
import os
from multiprocessing.queues import Queue

from zahir.base_types import Context
from zahir.constants import ZAHIR_LOG_OUTPUT_DIR_KEY, ZAHIR_START_JOB_TYPE_KEY
from zahir.events import (
    JobWorkerWaitingEvent,
    ZahirInternalErrorEvent,
)
from zahir.exception import exception_to_text_blob
from zahir.serialise import SerialisedEvent, serialise_event
from zahir.utils.logging_config import configure_logging
from zahir.utils.output_logging import setup_output_logging
from zahir.worker.state_machine import ZahirJobStateMachine, ZahirWorkerState
from zahir.worker.state_machine.states import StartStateChange

type OutputQueue = Queue[SerialisedEvent]
type InputQueue = Queue[SerialisedEvent]


def zahir_job_worker(context: Context, input_queue: InputQueue, output_queue: OutputQueue, workflow_id: str) -> None:
    """Receive and execute jobs dispatched by the overseer.

    Jobs are received via the input_queue. When the worker is ready for more work,
    it emits JobWorkerWaitingEvent to signal availability to the overseer.
    """

    # Configure logging for this worker process
    configure_logging()

    log_output_dir = context.state.get(ZAHIR_LOG_OUTPUT_DIR_KEY)
    start_job_type = context.state.get(ZAHIR_START_JOB_TYPE_KEY)
    if log_output_dir:
        setup_output_logging(log_output_dir=log_output_dir, start_job_type=start_job_type)

    context.job_registry.init(str(os.getpid()))
    atexit.register(context.job_registry.close)

    state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)
    current = StartStateChange({"message": "Starting job worker"})

    # Signal we're ready for work immediately
    output_queue.put(serialise_event(context, JobWorkerWaitingEvent(pid=os.getpid(), workflow_id=workflow_id)))

    # ...so I put a workflow engine inside your workflow engine
    while True:
        # We don't generally terminate this loop internally; the overseer process does based on completion events

        # run through our second secret workflow engine's steps repeatedly to update the job state
        handler = ZahirJobStateMachine.get_state(current.state)
        try:
            result = handler(state)
        except Exception as err:
            output_queue.put(
                serialise_event(
                    context,
                    ZahirInternalErrorEvent(
                        workflow_id=workflow_id,
                        error=exception_to_text_blob(err),
                    ),
                )
            )
            break

        if result is None:
            raise RuntimeError(f"ZahirJobStateMachine handler {handler} returned None unexpectedly")

        next_state, state = result
        current = next_state
