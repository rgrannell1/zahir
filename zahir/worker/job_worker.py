import multiprocessing
import os

from zahir.base_types import Context
from zahir.events import (
    ZahirEvent,
    ZahirInternalErrorEvent,
)
from zahir.exception import exception_to_text_blob
from zahir.worker.state_machine import ZahirJobStateMachine, ZahirWorkerState
from zahir.worker.state_machine.states import StartStateChange

type OutputQueue = multiprocessing.Queue["ZahirEvent"]


def zahir_job_worker(context: Context, output_queue: OutputQueue, workflow_id: str) -> None:
    """Repeatly request and execute jobs from the job registry until
    there's nothing else to be done. Communicate events back to the
    supervisor process.

    """

    context.job_registry.init(str(os.getpid()))

    state = ZahirWorkerState(context, output_queue, workflow_id)
    current = StartStateChange({"message": "Starting job worker"})

    # ...so I put a workflow engine inside your workflow engine
    while True:
        # We don't generally terminate this loop internally; the overseer process does based on completion events

        # run through our second secret workflow engine's steps repeatedly to update the job state
        handler = ZahirJobStateMachine.get_state(current.state)
        try:
            result = handler(state)
        except Exception as err:
            output_queue.put(
                ZahirInternalErrorEvent(
                    workflow_id=workflow_id,
                    error=exception_to_text_blob(err),
                )
            )
            break

        if result is None:
            raise RuntimeError(f"ZahirJobStateMachine handler {handler} returned None unexpectedly")

        next_state, state = result
        current = next_state
