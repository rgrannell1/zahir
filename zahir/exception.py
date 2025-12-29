"""Exceptions used throughout Zahir."""


class ZahirError(Exception):
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
