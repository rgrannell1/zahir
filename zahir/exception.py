"""Exceptions used throughout Zahir."""


class ZahirException(Exception):
    """Base exception for Zahir-related errors."""

    ...


class DependencyMissingException(ZahirException):
    """A dependency required by a job is missing."""

    ...
