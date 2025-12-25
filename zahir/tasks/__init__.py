"""Job registry implementations for workflow execution.

This package provides various job registry implementations for managing job
execution in workflows.
"""

from typing import Callable, Iterator, TypeVar, overload, TYPE_CHECKING
from zahir.tasks.retry import RetryOptions, RetryTaskInput, RetryTask, retryable

if TYPE_CHECKING:
    from zahir.types import Job, Context

T = TypeVar('T')


def job(run_func: T) -> T:
    """Decorator to convert a run function into a Job class.

    Takes a function with signature (context, input, dependencies) -> Iterator[Job | dict]
    and creates a Job class with that function as its run method.

    @param run_func: The function to use as the run method
    @return: A new Job class with the function as its run method
    
    Example:
        @job
        def ProcessFile(context, input, dependencies):
            with open(input["file_path"], "r") as file:
                for line in file:
                    print(line)
            return iter([])
    """
    from zahir.types import Job, Context
    
    class_name = run_func.__name__  # type: ignore

    class GeneratedJob(Job):
        @classmethod
        def run(cls, context: Context, input, dependencies) -> Iterator[Job | dict]:
            return run_func(context, input, dependencies)  # type: ignore

    GeneratedJob.__name__ = class_name
    GeneratedJob.__qualname__ = class_name
    GeneratedJob.__module__ = run_func.__module__  # type: ignore

    # Preserve the original function's docstring
    if hasattr(run_func, '__doc__') and run_func.__doc__:  # type: ignore
        GeneratedJob.__doc__ = run_func.__doc__  # type: ignore

    return GeneratedJob  # type: ignore


__all__ = [
    "RetryOptions",
    "RetryTaskInput",
    "RetryTask",
    "retryable",
    "job",
]
