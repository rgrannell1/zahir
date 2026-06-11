"""Runtime setup values for evaluate() and future remote process topologies."""

from collections.abc import Sequence
from typing import Any, Literal, TypeGuard

from tertius import IpcTransport, Transport

_RUNTIME_SIZE = 5

type NameProcessRef = tuple[Literal["name"], str]
type PidProcessRef = tuple[Literal["pid"], bytes]
type ProcessRef = NameProcessRef | PidProcessRef
type Runtime = tuple[Literal["runtime"], Transport, ProcessRef | None, tuple[ProcessRef, ...], int]


def setup(
    *,
    transport: Transport | None = None,
    overseer: ProcessRef | None = None,
    workers: Sequence[ProcessRef] = (),
    n_workers: int = 4,
) -> Runtime:
    """Return a tagged runtime value for evaluate().

    Process refs are accepted now so callers can adopt the future shape early.
    The current runtime still only implements the local spawn path.
    """

    selected_transport = transport if transport is not None else IpcTransport()
    return ("runtime", selected_transport, overseer, tuple(workers), n_workers)


def is_runtime(value: Any) -> TypeGuard[Runtime]:
    """Return True when the value is a tagged runtime tuple."""

    return (
        isinstance(value, tuple)
        and len(value) == _RUNTIME_SIZE
        and value[0] == "runtime"
        and isinstance(value[3], tuple)
        and isinstance(value[4], int)
    )
