"""Type stubs for tasks module."""

from typing import Callable, Iterator, TypeVar, overload
from zahir.types import Job, Context
from zahir.tasks.retry import RetryOptions, RetryTaskInput, RetryTask

T = TypeVar('T', bound=type[Job])

def job(run_func: Callable[[Context, object, object], Iterator[Job | dict]]) -> type[Job]: ...

def retryable(TaskClass: T) -> T: ...

__all__ = [
    "RetryOptions",
    "RetryTaskInput",
    "RetryTask",
    "retryable",
    "job",
]
