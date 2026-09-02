"""Defines dependencies that check path existence on the worker filesystem."""

from collections.abc import Generator
from functools import partial
from typing import Any

from zahir.core.coeffects import FileExists, provide_file_exists
from zahir.core.commons.constants import DependencyState
from zahir.core.commons.zahir_types import ConditionResult, DependencyResult
from zahir.core.dependencies.dependency import check, dependency


def classify_file_existence(fpath: str, exists: bool) -> ConditionResult:
    """Classify one path-existence observation."""
    metadata = {"path": fpath}
    if exists:
        return (DependencyState.SATISFIED, metadata)

    return (DependencyState.UNSATISFIED, metadata)


def file_condition(fpath: str) -> ConditionResult:
    """Return satisfied if a file exists, unsatisfied otherwise."""

    return classify_file_existence(fpath, provide_file_exists(FileExists(fpath)))


def request_file_condition(fpath: str) -> Generator[Any, Any, ConditionResult]:
    """Classify path existence supplied by the worker context."""

    exists = yield FileExists(fpath)
    return classify_file_existence(fpath, exists)


def file_dependency(fpath: str) -> Generator[Any, Any, DependencyResult]:
    """Poll until the file at fpath exists."""

    return dependency(
        partial(request_file_condition, fpath),
        label=f"file '{fpath}'",
    )


def check_file_dependency(fpath: str) -> Generator[Any, Any, DependencyResult]:
    """Check once whether the file at fpath exists; return impossible if it does not."""

    return check(
        partial(request_file_condition, fpath),
        label=f"file '{fpath}'",
    )
