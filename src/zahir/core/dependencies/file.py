# Dependency that checks a file exists on disk.
import pathlib
from collections.abc import Generator
from functools import partial
from typing import Any

from zahir.core.dependencies.dependency import check, dependency
from zahir.core.zahir_types import DependencyResult


def _file_condition(fpath: str) -> Generator[Any, Any, Any]:
    metadata = {"path": fpath}
    if pathlib.Path(fpath).exists():
        return (True, metadata)
    return False
    yield  # make it a generator function


def file_dependency(fpath: str) -> Generator[Any, Any, DependencyResult]:
    """Poll until the file at fpath exists."""
    return dependency(
        partial(_file_condition, fpath),
        label=f"file '{fpath}'",
    )


def check_file_dependency(fpath: str) -> Generator[Any, Any, DependencyResult]:
    """Check once whether the file at fpath exists; return impossible if it does not."""
    return check(
        partial(_file_condition, fpath),
        label=f"file '{fpath}'",
    )
