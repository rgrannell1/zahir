"""Types for worker runtime setup."""

from collections.abc import Sequence
from dataclasses import dataclass

from orbis import BindingMap
from tertius import Pid

from zahir.core.commons.zahir_types import HandlerMap
from zahir.core.evaluate.suspension import SuspensionTable, WorkerLocals

type WorkerBindings = tuple[HandlerMap, BindingMap]


@dataclass(frozen=True)
class WorkerHandlerOptions:
    """Inputs used to build a worker handler map."""

    suspension: SuspensionTable
    worker_locals: WorkerLocals
    overseer: Pid
    handler_wrappers: Sequence
    handlers: HandlerMap
    max_silence_ms: int | None
