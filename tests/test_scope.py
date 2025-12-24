"""Tests for LocalScope"""

from zahir.scope import LocalScope
from zahir.types import Job, Context
from zahir.dependencies.group import DependencyGroup
from zahir.dependencies.time import TimeDependency
from zahir.dependencies.job import JobDependency
from zahir.dependencies.concurrency import ConcurrencyLimit
from typing import Iterator


class SampleJob(Job):
    """Simple test job for scope tests."""

    @classmethod
    def run(cls, context: Context, input: dict, dependencies: DependencyGroup) -> Iterator[Job | dict]:
        yield {"result": "test"}


class AnotherSampleJob(Job):
    """Another test job for scope tests."""

    @classmethod
    def run(cls, context: Context, input: dict, dependencies: DependencyGroup) -> Iterator[Job | dict]:
        yield {"result": "another"}


def test_scope_add_and_get_task_class():
    """Test adding and retrieving task classes."""
    scope = LocalScope()

    scope.add_task_class(SampleJob)

    retrieved = scope.get_task_class("SampleJob")
    assert retrieved == SampleJob
    assert retrieved.__name__ == "SampleJob"


def test_scope_add_multiple_task_classes():
    """Test adding and retrieving multiple task classes."""
    scope = LocalScope()

    scope.add_task_class(SampleJob)
    scope.add_task_class(AnotherSampleJob)

    assert scope.get_task_class("SampleJob") == SampleJob
    assert scope.get_task_class("AnotherSampleJob") == AnotherSampleJob


def test_scope_get_task_class_raises_on_missing():
    """Test that getting a non-existent task class raises KeyError."""
    scope = LocalScope()

    try:
        scope.get_task_class("NonExistentJob")
        assert False, "Should have raised KeyError"
    except KeyError:
        pass


def test_scope_add_and_get_dependency_class():
    """Test adding and retrieving dependency classes."""
    scope = LocalScope()

    scope.add_dependency_class(TimeDependency)

    retrieved = scope.get_dependency_class("TimeDependency")
    assert retrieved == TimeDependency
    assert retrieved.__name__ == "TimeDependency"


def test_scope_add_multiple_dependency_classes():
    """Test adding and retrieving multiple dependency classes."""
    scope = LocalScope()

    scope.add_dependency_class(TimeDependency)
    scope.add_dependency_class(JobDependency)
    scope.add_dependency_class(ConcurrencyLimit)

    assert scope.get_dependency_class("TimeDependency") == TimeDependency
    assert scope.get_dependency_class("JobDependency") == JobDependency
    assert scope.get_dependency_class("ConcurrencyLimit") == ConcurrencyLimit


def test_scope_get_dependency_class_raises_on_missing():
    """Test that getting a non-existent dependency class raises KeyError."""
    scope = LocalScope()

    try:
        scope.get_dependency_class("NonExistentDependency")
        assert False, "Should have raised KeyError"
    except KeyError:
        pass


def test_scope_task_and_dependency_classes_separate():
    """Test that task and dependency classes are stored separately."""
    scope = LocalScope()

    scope.add_task_class(SampleJob)
    scope.add_dependency_class(TimeDependency)

    # Should be able to retrieve both independently
    assert scope.get_task_class("SampleJob") == SampleJob
    assert scope.get_dependency_class("TimeDependency") == TimeDependency

    # Tasks shouldn't be in dependencies and vice versa
    try:
        scope.get_dependency_class("SampleJob")
        assert False, "Should have raised KeyError"
    except KeyError:
        pass

    try:
        scope.get_task_class("TimeDependency")
        assert False, "Should have raised KeyError"
    except KeyError:
        pass


def test_scope_overwrite_task_class():
    """Test that adding a task class with the same name overwrites the previous one."""
    scope = LocalScope()

    scope.add_task_class(SampleJob)
    original = scope.get_task_class("SampleJob")

    # Add again (same class)
    scope.add_task_class(SampleJob)
    retrieved = scope.get_task_class("SampleJob")

    assert retrieved == original
    assert retrieved == SampleJob


def test_scope_empty_initialization():
    """Test that a new scope starts empty."""
    scope = LocalScope()

    assert len(scope.jobs) == 0
    assert len(scope.dependencies) == 0
