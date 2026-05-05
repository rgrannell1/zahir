"""Zahir's exceptions"""

class ZahirError(Exception):
    pass


class JobTimeoutError(ZahirError):
    pass


class JobError(ZahirError):
    def __init__(self, cause: Exception) -> None:
        self.cause = cause
        super().__init__(str(cause))


class InvalidEffectError(ZahirError):
    """Not an effect."""
    pass


class ImpossibleError(ZahirError):
    """Raised by a condition function to signal the dependency can never be satisfied."""
