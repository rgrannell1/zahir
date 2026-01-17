"""Tests for LocalScope"""

from zahir.base_types import Context, JobSpec
from zahir.dependencies.concurrency import ConcurrencyLimit
from zahir.dependencies.group import DependencyGroup
from zahir.dependencies.job import JobDependency
from zahir.dependencies.time import TimeDependency
from zahir.events import JobOutputEvent
from zahir.exception import DependencyNotInScopeError, JobNotInScopeError
from zahir.jobs.decorator import spec
from zahir.scope import LocalScope


@spec()
def SampleJobSpec(spec_args, context: Context, input: dict, dependencies: DependencyGroup):
    """Simple test job for scope tests."""
    yield JobOutputEvent({"result": "test"})


@spec()
def AnotherSampleJobSpec(spec_args, context: Context, input: dict, dependencies: DependencyGroup):
    """Another test job for scope tests."""
    yield JobOutputEvent({"result": "another"})


def test_scope_add_and_get_job_spec():
    """Test adding and retrieving job specs."""
    scope = LocalScope()

    scope.add_job_spec(SampleJobSpec)

    retrieved = scope.get_job_spec("SampleJobSpec")
    assert retrieved == SampleJobSpec
    assert retrieved.type == "SampleJobSpec"


def test_scope_add_multiple_job_specs():
    """Test adding and retrieving multiple job specs."""
    scope = LocalScope()

    scope.add_job_spec(SampleJobSpec)
    scope.add_job_spec(AnotherSampleJobSpec)

    assert scope.get_job_spec("SampleJobSpec") == SampleJobSpec
    assert scope.get_job_spec("AnotherSampleJobSpec") == AnotherSampleJobSpec


def test_scope_get_job_spec_raises_on_missing():
    """Test that getting a non-existent job spec raises JobNotInScopeError."""
    scope = LocalScope()

    try:
        scope.get_job_spec("NonExistentJobSpec")
        raise AssertionError("failed")
    except JobNotInScopeError:
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
    """Test that getting a non-existent dependency class raises DependencyNotInScopeError."""
    scope = LocalScope()

    try:
        scope.get_dependency_class("NonExistentDependency")
        raise AssertionError("failed")
    except DependencyNotInScopeError:
        pass


def test_scope_job_and_dependency_classes_separate():
    """Test that job specs and dependency classes are stored separately."""
    scope = LocalScope()

    scope.add_job_spec(SampleJobSpec)
    scope.add_dependency_class(TimeDependency)

    # Should be able to retrieve both independently
    assert scope.get_job_spec("SampleJobSpec") == SampleJobSpec
    assert scope.get_dependency_class("TimeDependency") == TimeDependency

    # Specs shouldn't be in dependencies and vice versa
    try:
        scope.get_dependency_class("SampleJobSpec")
        raise AssertionError("failed")
    except DependencyNotInScopeError:
        pass

    try:
        scope.get_job_spec("TimeDependency")
        raise AssertionError("failed")
    except JobNotInScopeError:
        pass


def test_scope_overwrite_job_spec():
    """Test that adding a job spec with the same name overwrites the previous one."""
    scope = LocalScope()

    scope.add_job_spec(SampleJobSpec)
    original = scope.get_job_spec("SampleJobSpec")

    # Add again (same spec)
    scope.add_job_spec(SampleJobSpec)
    retrieved = scope.get_job_spec("SampleJobSpec")

    assert retrieved == original
    assert retrieved == SampleJobSpec


def test_scope_empty_initialization():
    """Test that a new scope starts empty."""
    scope = LocalScope()

    assert len(scope.specs) == 0
    assert len(scope.dependencies) == 0
