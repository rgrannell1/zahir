"""Exceptions used throughout Zahir."""

import base64
import json
import pickle  # noqa: S403
import traceback
from typing import Any


class ZahirError(Exception):
    """Base exception for Zahir-related errors."""


class ZahirInternalError(ZahirError):
    """An internal Zahir error has occurred."""


class DuplicateJobError(ZahirInternalError):
    """A job with the same ID already exists in the registry."""


class MissingJobError(ZahirInternalError):
    """A job could not be found in the registry."""


class JobPrecheckError(ZahirError):
    """A job precheck has failed."""


class ImpossibleDependencyError(ZahirError):
    """A job has an impossible dependency."""


class DependencyMissingError(ZahirError):
    """A dependency required by a job is missing."""


class JobTimeoutError(ZahirError):
    """A job has exceeded its allowed execution time."""


class JobRecoveryTimeoutError(ZahirError):
    """A job recovery attempt has exceeded its allowed time."""


class NotInScopeError(ZahirError):
    """A class was not found in the current scope."""


class JobNotInScopeError(NotInScopeError):
    """A job class was not found in the current scope."""


class DependencyNotInScopeError(NotInScopeError):
    """A dependency class was not found in the current scope."""


class TransformNotInScopeError(NotInScopeError):
    """A transform was not found in the current scope."""


def exception_to_text_blob(exc: Exception) -> str:
    """Serialize an exception + traceback to a text blob."""

    # Format traceback as a string since TracebackException can't be pickled (contains code objects)
    tb_lines = traceback.format_exception(type(exc), exc, exc.__traceback__)
    tb_string = "".join(tb_lines)

    payload: dict[str, Any] = {
        "exception": base64.b64encode(pickle.dumps(exc, protocol=pickle.HIGHEST_PROTOCOL)).decode("ascii"),
        "traceback": tb_string,
    }

    return base64.b64encode(json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf8")).decode(
        "ascii"
    )


def exception_from_text_blob(blob: str) -> Exception:
    """Deserialize an exception + traceback from a text blob."""

    payload = json.loads(base64.b64decode(blob.encode("ascii")).decode("utf8"))

    exc = pickle.loads(base64.b64decode(payload["exception"].encode("ascii")))

    if not isinstance(exc, Exception):
        raise TypeError(f"Unpickled object is not an exception: {type(exc)!r}")

    # Attach the formatted traceback string to the exception for display
    exc.__serialized_traceback__ = payload["traceback"]
    return exc
