
import multiprocessing
from zahir.base_types import Scope
from zahir.events import (
    ZahirEvent,
)

type OutputQueue = multiprocessing.Queue["ZahirEvent"]

def zahir_dependency_worker(
    scope: Scope, output_queue: OutputQueue, workflow_id: str
) -> None:
    """Analyse job dependencies and mark jobs as pending."""
    ...
