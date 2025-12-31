"""Exceptions used throughout Zahir."""

import base64
import pickle

class ZahirError(BaseException):
    """Base exception for Zahir-related errors."""


class JobPrecheckError(ZahirError):
    """A job precheck has failed."""


class DependencyMissingError(ZahirError):
    """A dependency required by a job is missing."""


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
