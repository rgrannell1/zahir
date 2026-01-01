"""Exceptions used throughout Zahir."""

import base64
import pickle


class ZahirError(BaseException):
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


def exception_to_text_blob(exception: BaseException) -> str:
    """Serialize an exception to a text blob."""

    pickled = pickle.dumps(exception, protocol=pickle.HIGHEST_PROTOCOL)

    return base64.b64encode(pickled).decode("ascii")


def exception_from_text_blob(blob: str) -> BaseException:
    """Deserialize an exception from a text blob."""

    pickled = base64.b64decode(blob.encode("ascii"))
    restored = pickle.loads(pickled)

    if not isinstance(restored, BaseException):
        raise TypeError(f"Unpickled object is not an exception: {type(restored)!r}")

    return restored
