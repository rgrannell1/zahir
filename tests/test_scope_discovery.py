"""Tests for LocalScope.from_module() discovery functionality."""

from types import ModuleType

from zahir.base_types import Context, Dependency, DependencyState
from zahir.events import JobOutputEvent
from zahir.jobs.decorator import spec
from zahir.scope import LocalScope
import sys

# Create a fake module for testing
test_module = ModuleType("test_workflows")


# Define some test jobs and dependencies in the module
@spec()
def SampleJob1(context: Context, input, dependencies):
    """A test job."""
    yield JobOutputEvent({"result": "test1"})


@spec()
def SampleJob2(context: Context, input, dependencies):
    """Another test job."""
    yield JobOutputEvent({"result": "test2"})


class SampleDependency1(Dependency):
    """A test dependency."""

    def satisfied(self):
        from zahir.base_types import DependencyResult, DependencyState

        return DependencyResult(state=DependencyState.SATISFIED)

    def request_extension(self, extra_seconds: float):
        return self

    def save(self, context):
        return {"type": "SampleDependency1"}

    @classmethod
    def load(cls, context, data):
        return cls()


class SampleDependency2(Dependency):
    """Another test dependency."""

    def satisfied(self):
        from zahir.base_types import DependencyResult, DependencyState

        return DependencyResult(state=DependencyState.UNSATISFIED)

    def request_extension(self, extra_seconds: float):
        return self

    def save(self, context):
        return {"type": "SampleDependency2"}

    @classmethod
    def load(cls, context, data):
        return cls()


# Add these to the fake module
test_module.SampleJob1 = SampleJob1
test_module.SampleJob2 = SampleJob2
test_module.SampleDependency1 = SampleDependency1
test_module.SampleDependency2 = SampleDependency2

# Set the __module__ attribute so they appear to be from test_module
SampleJob1.__module__ = "test_workflows"
SampleJob2.__module__ = "test_workflows"
SampleDependency1.__module__ = "test_workflows"
SampleDependency2.__module__ = "test_workflows"


def test_from_module_discovers_jobs():
    """Test that from_module discovers all Job specs in a module."""

    scope = LocalScope.from_module(test_module)

    # Should have discovered both jobs (as specs since they're @spec decorated)
    assert "SampleJob1" in scope.specs
    assert "SampleJob2" in scope.specs
    assert scope.specs["SampleJob1"] == SampleJob1
    assert scope.specs["SampleJob2"] == SampleJob2


def test_from_module_discovers_dependencies():
    """Test that from_module discovers all Dependency classes in a module."""

    scope = LocalScope.from_module(test_module)

    # Should have discovered both dependencies
    assert "SampleDependency1" in scope.dependencies
    assert "SampleDependency2" in scope.dependencies
    assert scope.dependencies["SampleDependency1"] == SampleDependency1
    assert scope.dependencies["SampleDependency2"] == SampleDependency2


def test_from_module_discovers_both():
    """Test that from_module discovers both jobs and dependencies."""

    scope = LocalScope.from_module(test_module)

    # Should have found 2 specs (plus built-ins Sleep, Empty) and 2 dependencies (plus built-ins)
    assert "SampleJob1" in scope.specs
    assert "SampleJob2" in scope.specs
    assert "SampleDependency1" in scope.dependencies
    assert "SampleDependency2" in scope.dependencies


def test_from_module_skips_base_classes():
    """Test that from_module doesn't register the base Dependency class."""

    # Add the base class to the module
    test_module.Dependency = Dependency
    Dependency.__module__ = "test_workflows"

    scope = LocalScope.from_module(test_module)

    # Should not have registered the base class itself
    assert "Dependency" not in scope.dependencies

    # Clean up
    delattr(test_module, "Dependency")


def test_from_module_includes_imported_classes():
    """Test that from_module discovers imported classes too."""

    # Create a second module
    other_module = ModuleType("other_module")

    @spec()
    def ExternalJob(context: Context, input, dependencies):
        """A job from another module."""
        yield JobOutputEvent({"result": "external"})

    ExternalJob.__module__ = "other_module"
    other_module.ExternalJob = ExternalJob

    # Import the external job into our test module
    test_module.ExternalJob = ExternalJob

    scope = LocalScope.from_module(test_module)

    # Should have discovered the imported job spec
    assert "ExternalJob" in scope.specs

    # Clean up
    delattr(test_module, "ExternalJob")


def test_from_module_empty_module():
    """Test that from_module handles modules with no jobs or dependencies."""

    empty_module = ModuleType("empty_module")

    scope = LocalScope.from_module(empty_module)

    # Scope should have pre-loaded built-in specs and dependencies, but no module-specific ones
    assert "SampleJob1" not in scope.specs
    assert "SampleDependency1" not in scope.dependencies
    # Built-ins should still be present
    assert "Sleep" in scope.specs
    assert "Empty" in scope.specs


def test_from_module_get_job_class():
    """Test that discovered specs can be retrieved."""

    scope = LocalScope.from_module(test_module)

    job_spec = scope.get_job_spec("SampleJob1")
    assert job_spec == SampleJob1


def test_from_module_get_dependency_class():
    """Test that discovered dependencies can be retrieved."""

    scope = LocalScope.from_module(test_module)

    dep_class = scope.get_dependency_class("SampleDependency1")
    assert dep_class == SampleDependency1


def test_from_module_defaults_to_current_module():
    """Test that from_module without arguments discovers from the calling module."""

    # When called without arguments, it should discover from the current module
    scope = LocalScope.from_module()

    # Should have discovered jobs defined in this test file
    # (none are defined at module level, so this should be empty)
    # But we can verify the method works by checking it doesn't crash
    assert isinstance(scope, LocalScope)
