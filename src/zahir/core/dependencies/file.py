# Dependency that checks a file exists on disk.
import pathlib
from collections.abc import Generator
from functools import partial
from typing import Any

from zahir.core.constants import DependencyState
from zahir.core.dependencies.dependency import check, dependency
from zahir.core.zahir_types import ConditionResult, DependencyResult


def file_condition(fpath: str) -> Generator[Any, Any, ConditionResult]:
    """Return satified if a file exists, unsatisfied otherwise."""

    metadata = {"path": fpath}
    if pathlib.Path(fpath).exists():
        return (DependencyState.SATISFIED, metadata)

    yield from ()  # make it a generator function
    return (DependencyState.UNSATISFIED, metadata)


def file_dependency(fpath: str) -> Generator[Any, Any, DependencyResult]:
    """Poll until the file at fpath exists."""

    return dependency(
        partial(file_condition, fpath),
        label=f"file '{fpath}'",
    )


def check_file_dependency(fpath: str) -> Generator[Any, Any, DependencyResult]:
    """Check once whether the file at fpath exists; return impossible if it does not."""

    return check(
        partial(file_condition, fpath),
        label=f"file '{fpath}'",
    )
