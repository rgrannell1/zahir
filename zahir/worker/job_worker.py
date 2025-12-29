import multiprocessing

from zahir.base_types import Context
from zahir.events import (
    ZahirEvent,
)
from zahir.worker.job_state_machine import StateChange, ZahirJobState, ZahirJobStateMachine, ZahirWorkerState

type OutputQueue = multiprocessing.Queue["ZahirEvent"]

def zahir_job_worker(context: Context, output_queue: OutputQueue, workflow_id: str) -> None:
    """Repeatly request and execute jobs from the job registry until
    there's nothing else to be done. Communicate events back to the
    supervisor process.

    """

    state = ZahirWorkerState(context, output_queue, workflow_id)
    current = StateChange(ZahirJobState.START, {"message": "Starting job worker"})

    # ...so I put a workflow engine inside your workflow engine
    while True:
        # We don't terminate this loop internally; the overseer process does based on completion events

        # run through our second secret workflow engine's steps repeatedly to update the job state
        handler = ZahirJobStateMachine.get_state(current.state)
        result = handler(state)

        if result is None:
            raise RuntimeError(f"ZahirJobStateMachine handler {handler} returned None unexpectedly")

        next_state, state = result
        current = next_state
