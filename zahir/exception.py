"""Exceptions used throughout Zahir."""


class ZahirException(Exception):
    """Base exception for Zahir-related errors."""

    ...


class DependencyMissingException(ZahirException):
    """A dependency required by a job is missing."""

    ...


class TimeoutException(ZahirException):
    """A job or recovery has exceeded its allowed time limit."""

    ...


class UnrecoverableJobException(ZahirException):
    """A job has failed and cannot be recovered."""

    ...
