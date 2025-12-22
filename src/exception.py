"""Exceptions used throughout Zahir."""


class ZahirException(Exception):
    """Base exception for Zahir-related errors."""

    ...


class DependencyMissingException(ZahirException):
    """A dependency required by a task is missing."""

    ...


class TimeoutException(ZahirException):
    """A task or recovery has exceeded its allowed time limit."""

    ...
