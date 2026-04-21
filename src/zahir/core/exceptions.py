# Structured Left types for the zahir exception hierarchy — all subclasses pass through the worker unwrapped.
class ZahirException(Exception):
    pass


class JobTimeout(ZahirException):
    pass


class JobError(ZahirException):
    def __init__(self, cause: Exception) -> None:
        self.cause = cause
        super().__init__(str(cause))


class InvalidEffect(ZahirException):
    pass


class ImpossibleError(ZahirException):
    """Raised by a condition function to signal the dependency can never be satisfied."""
