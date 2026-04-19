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

