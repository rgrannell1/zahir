"""Effects yielded by the worker/runtime layer — never by user job code directly."""

from dataclasses import dataclass
from typing import Any, ClassVar, LiteralString

from orbis import Effect

from zahir.core.zahir_types import JobSpec


class ZahirCoordinationEffect[ReturnT](Effect[ReturnT], abstract=True):
    """Base class for effects yielded by the runtime layer, never by jobs directly."""


@dataclass
class EEnqueue(ZahirCoordinationEffect[None]):
    """Internal: enqueue a child job and route its reply back to this worker.

    The JobSpec travels whole — reply_to and sequence_number on it route the
    child's result back to the enqueuing worker; both are None for the root job.
    """

    tag: ClassVar[LiteralString] = "enqueue"
    job: JobSpec


@dataclass
class EGetJob(ZahirCoordinationEffect[Any]):
    """Internal: request work from the overseer — returns a new job, a buffered result, or None."""

    tag: ClassVar[LiteralString] = "get_job"
    worker_pid_bytes: bytes = b""


@dataclass
class EJobComplete(ZahirCoordinationEffect[None]):
    """Internal: report successful job completion and route the result to the caller."""

    tag: ClassVar[LiteralString] = "job_complete"
    result: Any
    reply_to: bytes | None
    sequence_number: Any
    fn_name: str = ""


@dataclass
class EJobFail(ZahirCoordinationEffect[None]):
    """Internal: report job failure (timeout or error) and route the exception to the caller."""

    tag: ClassVar[LiteralString] = "job_fail"
    error: Exception
    reply_to: bytes | None
    sequence_number: Any
    fn_name: str = ""
