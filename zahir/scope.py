import inspect
from types import ModuleType
from typing import Self

from zahir.base_types import Dependency, JobSpec, Scope, Transform
from zahir.dependencies.concurrency import ConcurrencyLimit
from zahir.dependencies.group import DependencyGroup
from zahir.dependencies.job import JobDependency
from zahir.dependencies.resources import ResourceLimit
from zahir.dependencies.semaphore import Semaphore
from zahir.dependencies.time import TimeDependency
from zahir.exception import DependencyNotInScopeError, JobNotInScopeError, TransformNotInScopeError
from zahir.jobs import Empty, Sleep
from zahir.transforms.retry import retry


def _find_caller_module() -> ModuleType | None:
    """Find the calling module by walking up the call stack.

    Skips internal zahir modules and pytest modules to find the actual caller.

    @return: The calling module, or None if not found
    """
    frame = inspect.currentframe()
    if frame is None:
        return None

    # Walk up the stack to find a frame with a valid module
    # Skip internal zahir modules and pytest modules
    current_frame = frame.f_back  # Skip this function's frame
    while current_frame is not None:
        candidate_module = inspect.getmodule(current_frame)
        if candidate_module is not None:
            module_name = candidate_module.__name__
            # Skip zahir internal modules and pytest modules
            if not module_name.startswith(("zahir.", "_pytest", "pytest")):
                return candidate_module
        current_frame = current_frame.f_back

    return None

# Register built in job specs
INTERNAL_JOB_SPECS = {"Sleep": Sleep, "Empty": Empty}

# Why make people work harder; just register all the built-in dependencies for free
INTERNAL_DEPENDENCIES: dict[str, type[Dependency]] = {
    "ConcurrencyLimit": ConcurrencyLimit,
    "DependencyGroup": DependencyGroup,
    "JobDependency": JobDependency,
    "ResourceLimit": ResourceLimit,
    "Semaphore": Semaphore,
    "TimeDependency": TimeDependency,
}

# Built-in transforms available by default
INTERNAL_TRANSFORMS: dict[str, Transform] = {
    "retry": retry,
}


class LocalScope(Scope):
    """A local translation layer between dependency / job names and
    their underlying Python classes."""

    def __init__(
        self,
        dependencies: list[type[Dependency]] | None = None,
        specs: list["JobSpec"] | None = None,
        transforms: dict[str, Transform] | None = None,
        _source_module: ModuleType | str | None = None,
    ) -> None:
        self.dependencies: dict[str, type[Dependency]] = INTERNAL_DEPENDENCIES.copy()
        self.specs: dict[str, JobSpec] = INTERNAL_JOB_SPECS.copy()
        self.transforms: dict[str, Transform] = INTERNAL_TRANSFORMS.copy()
        # Store module reference for pickling/unpickling
        self._source_module: ModuleType | str | None = _source_module

        if dependencies:
            self.add_dependency_classes(dependencies)
        if specs:
            self.add_job_specs(specs)

        if transforms:
            for name, transform in transforms.items():
                self.add_transform(name, transform)

    def add_job_spec(self, spec: "JobSpec") -> Self:
        self.specs[spec.type] = spec
        return self

    def add_job_specs(self, specs: list["JobSpec"]) -> Self:
        for spec in specs:
            self.specs[spec.type] = spec
        return self

    def add_job_class(self, job_spec: JobSpec) -> Self:
        """Add a job spec to the scope.

        @param job_spec: The job spec to add.
        """

        self.specs[job_spec.type] = job_spec
        return self

    def add_job_classes(self, job_specs: list[JobSpec]) -> Self:
        """Add multiple job specs to the scope.

        @param job_specs: The job specs to add.
        """

        for job_spec in job_specs:
            self.specs[job_spec.type] = job_spec
        return self

    def get_job_spec(self, type_name: str) -> JobSpec:
        """Get a job spec from the scope by name.

        @param type_name: The name of the job spec to get.
        """

        if type_name not in self.specs:
            raise JobNotInScopeError(f"Job spec '{type_name}' not found in scope. Did you register it?")

        return self.specs[type_name]

    def add_dependency_class(self, dependency_class: type[Dependency]) -> Self:
        """Add a dependency class to the scope.

        @param dependency_class: The dependency class to add.
        """

        self.dependencies[dependency_class.__name__] = dependency_class
        return self

    def add_dependency_classes(self, dependency_classes: list[type[Dependency]]) -> Self:
        """Add multiple dependency classes to the scope.

        @param dependency_classes: The dependency classes to add.
        """

        for dependency_class in dependency_classes:
            self.dependencies[dependency_class.__name__] = dependency_class
        return self

    def get_dependency_class(self, type_name: str) -> type[Dependency]:
        """Get a dependency class from the scope by name.

        @param type_name: The name of the dependency class to get.
        """

        if type_name not in self.dependencies:
            raise DependencyNotInScopeError(f"Dependency class '{type_name}' not found in scope. Did you register it?")

        return self.dependencies[type_name]

    def get_transform(self, type_name: str) -> Transform:
        """Get a transform function from the scope by name.

        @param type_name: The name of the transform to get.
        """

        if type_name not in self.transforms:
            raise TransformNotInScopeError(f"Transform '{type_name}' not found in scope. Did you register it?")

        return self.transforms[type_name]

    def add_transform(self, type_name: str, transform: Transform) -> Self:
        """Add a transform function to the scope.

        @param type_name: The name to register the transform under.
        @param transform: The transform function.
        """

        self.transforms[type_name] = transform
        return self

    @classmethod
    def from_module(cls, module: ModuleType | None = None) -> Self:
        """Create a LocalScope by discovering all Job and Dependency classes in a module.

        This method will automatically find and register all classes that inherit from
        Job or Dependency in the given module. If no module is provided, it will
        discover from the calling module.

        @param module: The Python module to search for Job and Dependency classes.
                      If None, discovers from the calling module.
        @return: A new LocalScope instance with discovered classes registered

        Examples:
            # Discover from a specific module
            import my_workflows
            scope = LocalScope.from_module(my_workflows)

            # Discover from current module
            scope = LocalScope.from_module()
        """

        if module is None:
            module = _find_caller_module()
            if module is None:
                raise RuntimeError("Cannot determine calling module")

        scope = cls(_source_module=module)
        scope._populate_from_module(module)
        return scope
    
    def __getstate__(self) -> dict:
        """Custom pickling: serialize only the module name, not the functions."""
        # Store module name instead of module object
        module_name: str | None = None
        if self._source_module is not None:
            if isinstance(self._source_module, ModuleType):
                module_name = self._source_module.__name__
            else:
                module_name = str(self._source_module)
        
        return {
            "_source_module": module_name,
            # Store spec type names (functions will be reconstructed from module)
            "_spec_type_names": list(self.specs.keys()),
            # Store dependency class names (classes will be reconstructed from module)
            "_dependency_class_names": list(self.dependencies.keys()),
            # Store transform names (functions will be reconstructed from module)
            "_transform_names": list(self.transforms.keys()),
        }
    
    def _populate_from_module(self, module: ModuleType) -> None:
        """Populate this scope by discovering JobSpecs and Dependencies from a module."""
        specs: list[JobSpec] = []
        dependencies: list[type[Dependency]] = []

        for _, obj in inspect.getmembers(module):
            if isinstance(obj, JobSpec):
                specs.append(obj)

            if not inspect.isclass(obj):
                continue

            if issubclass(obj, Dependency) and obj is not Dependency:
                dependencies.append(obj)

        if dependencies:
            self.add_dependency_classes(dependencies)
        if specs:
            self.add_job_specs(specs)

    def __setstate__(self, state: dict) -> None:
        """Custom unpickling: reconstruct scope from module."""
        # Initialize with defaults
        self.dependencies = INTERNAL_DEPENDENCIES.copy()
        self.specs = INTERNAL_JOB_SPECS.copy()
        self.transforms = INTERNAL_TRANSFORMS.copy()
        
        module_name = state.get("_source_module")
        
        # Reconstruct from module if available
        if module_name:
            try:
                import importlib
                import sys
                
                # Handle __main__ specially - try to get it from sys.modules
                if module_name == "__main__":
                    module = sys.modules.get("__main__")
                    if module is not None and hasattr(module, "__file__"):
                        # We have the main module, try to populate from it
                        try:
                            self._populate_from_module(module)
                            self._source_module = module
                        except Exception:
                            # If population fails, keep defaults
                            self._source_module = module_name
                    else:
                        # Can't get main module, keep defaults
                        self._source_module = module_name
                else:
                    # Regular module - try to import it
                    try:
                        module = importlib.import_module(module_name)
                        # Populate scope from module
                        self._populate_from_module(module)
                        self._source_module = module
                    except ImportError:
                        # Module not importable - keep defaults
                        self._source_module = module_name
            except Exception:
                # Any other error - fallback to defaults
                self._source_module = module_name
        else:
            # No module info, use defaults
            self._source_module = None
