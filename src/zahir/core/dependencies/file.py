# Dependency that checks a file exists on disk.
import pathlib
from collections.abc import Generator
from functools import partial
from typing import Any

from zahir.core.combinators import lift
from zahir.core.commons.constants import DependencyState
from zahir.core.commons.zahir_types import ConditionResult, DependencyResult
from zahir.core.dependencies.dependency import check, dependency


def file_condition(fpath: str) -> ConditionResult:
    """Return satisfied if a file exists, unsatisfied otherwise."""

    metadata = {"path": fpath}
    if pathlib.Path(fpath).exists():
        return (DependencyState.SATISFIED, metadata)

    return (DependencyState.UNSATISFIED, metadata)


def file_dependency(fpath: str) -> Generator[Any, Any, DependencyResult]:
    """Poll until the file at fpath exists."""

    return dependency(
        partial(lift, file_condition, fpath),
        label=f"file '{fpath}'",
    )


def check_file_dependency(fpath: str) -> Generator[Any, Any, DependencyResult]:
    """Check once whether the file at fpath exists; return impossible if it does not."""

    return check(
        partial(lift, file_condition, fpath),
        label=f"file '{fpath}'",
    )
