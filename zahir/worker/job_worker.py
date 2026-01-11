import multiprocessing
import os

from zahir.base_types import Context
from zahir.events import (
    JobWorkerWaitingEvent,
    ZahirEvent,
    ZahirInternalErrorEvent,
)
from zahir.exception import exception_to_text_blob
from zahir.worker.state_machine import ZahirJobStateMachine, ZahirWorkerState
from zahir.worker.state_machine.states import StartStateChange

type OutputQueue = multiprocessing.Queue["ZahirEvent"]
type InputQueue = multiprocessing.Queue["ZahirEvent"]


def zahir_job_worker(context: Context, input_queue: InputQueue, output_queue: OutputQueue, workflow_id: str) -> None:
    """Receive and execute jobs dispatched by the overseer.

    Jobs are received via the input_queue. When the worker is ready for more work,
    it emits JobWorkerWaitingEvent to signal availability to the overseer.
    """

    context.job_registry.init(str(os.getpid()))

    state = ZahirWorkerState(context, input_queue, output_queue, workflow_id)
    current = StartStateChange({"message": "Starting job worker"})

    # Signal we're ready for work immediately
    output_queue.put(JobWorkerWaitingEvent(pid=os.getpid()))

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
