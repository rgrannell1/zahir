"""Types for remote worker setup."""

from dataclasses import dataclass

from orbis import BindingMap
from tertius import CurveSecurity, Scope

from zahir.core.commons.zahir_types import HandlerMap
from zahir.core.evaluate.worker_types import InterpreterWrappers


@dataclass(frozen=True)
class RemoteConnection:
    """TCP connection details for a remote worker."""

    host: str
    data_port: int
    control_port: int
    security: CurveSecurity | None


@dataclass(frozen=True)
class RemoteTimeouts:
    """Timeouts used by a remote worker."""

    overseer_ms: int
    receive_ms: int


@dataclass(frozen=True)
class RemoteWorkerOptions:
    """Inputs used to start a remote worker."""

    connection: RemoteConnection
    scope: Scope
    wrappers: InterpreterWrappers
    handlers: HandlerMap
    providers: BindingMap
    timeouts: RemoteTimeouts
