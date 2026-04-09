"""Tests for LocalScope.scan()."""

from pathlib import Path

import pytest

from zahir.base_types import Context, Dependency, DependencyResult, DependencyState
from zahir.events import JobOutputEvent
from zahir.jobs.decorator import spec
from zahir.scope import LocalScope


_JOB_AND_DEP_PY = """
from zahir.base_types import Context, Dependency, DependencyResult, DependencyState
from zahir.events import JobOutputEvent
from zahir.jobs.decorator import spec

@spec()
def ScannedJob(context: Context, input, dependencies):
    yield JobOutputEvent({"ok": True})


class ScannedDep(Dependency):
    def satisfied(self):
        return DependencyResult(state=DependencyState.SATISFIED)

    def request_extension(self, extra_seconds: float):
        return self

    def save(self, context):
        return {"type": "ScannedDep"}

    @classmethod
    def load(cls, context, data):
        return cls()
"""


def test_scan_flat_module(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / "jobs.py").write_text(_JOB_AND_DEP_PY)

    scope = LocalScope()
    scope.scan(root)

    assert "ScannedJob" in scope.specs
    assert "ScannedDep" in scope.dependencies


def test_scan_nested_namespace_package(tmp_path: Path) -> None:
    root = tmp_path / "root"
    sub = root / "sub"
    sub.mkdir(parents=True)
    (sub / "tasks.py").write_text(_JOB_AND_DEP_PY)

    scope = LocalScope()
    scope.scan(root)

    assert "ScannedJob" in scope.specs
    assert "ScannedDep" in scope.dependencies


def test_scan_merges_with_existing(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / "more.py").write_text(_JOB_AND_DEP_PY)

    scope = LocalScope()
    assert "Sleep" in scope.specs
    scope.scan(root)
    assert "Sleep" in scope.specs
    assert "ScannedJob" in scope.specs


def test_scan_chaining_returns_self(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / "jobs.py").write_text(_JOB_AND_DEP_PY)

    s = LocalScope().scan(root)
    assert isinstance(s, LocalScope)
    assert "ScannedJob" in s.specs


def test_scan_not_a_directory(tmp_path: Path) -> None:
    f = tmp_path / "file.txt"
    f.write_text("x")
    with pytest.raises(NotADirectoryError):
        LocalScope().scan(f)


def test_scan_missing_path() -> None:
    with pytest.raises(FileNotFoundError):
        LocalScope().scan("/nonexistent/zahir_scan_path_12345")


def test_scan_import_error_wraps(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / "broken.py").write_text("raise RuntimeError('boom')\n")

    with pytest.raises(ImportError, match="scan: failed to import 'broken'"):
        LocalScope().scan(root)


def test_scan_discovers_root_init_module(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / "__init__.py").write_text(_JOB_AND_DEP_PY)

    scope = LocalScope()
    scope.scan(root)

    assert "ScannedJob" in scope.specs
    assert "ScannedDep" in scope.dependencies
