import inspect
from types import ModuleType
from typing import Self

from zahir.base_types import Dependency, JobSpec, Scope
from zahir.exception import DependencyNotInScopeError, JobNotInScopeError


class LocalScope(Scope):
    """A local translation layer between dependency / job names and
    their underlying Python classes."""

    def __init__(
        self, dependencies: list[type[Dependency]] = [], specs: list["JobSpec"] = []
    ) -> None:
        self.dependencies: dict[str, type[Dependency]] = {}
        self.specs: dict[str, "JobSpec"] = {}

        self.add_dependency_classes(dependencies)
        self.add_job_specs(specs)

    def add_job_spec(self, spec: "JobSpec") -> Self:
        self.specs[spec.type] = spec
        return self

    def add_job_specs(self, specs: list["JobSpec"]) -> Self:
        for spec in specs:
            self.specs[spec.type] = spec
        return self

    def get_job_spec(self, type: str) -> JobSpec:
        if not type in self.specs:
            raise KeyError(f"Job spec '{type}' not found in scope. Did you register it?")
        return self.specs[type]

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
            # Get the caller's frame and extract their module
            frame = inspect.currentframe()
            if frame is None:
                raise RuntimeError("Cannot determine calling module")

            # Walk up the stack to find a frame with a valid module
            # Skip internal zahir modules and pytest modules
            current_frame = frame
            while current_frame is not None:
                candidate_module = inspect.getmodule(current_frame)
                if candidate_module is not None:
                    module_name = candidate_module.__name__
                    # Skip zahir internal modules and pytest modules
                    if not module_name.startswith(("zahir.", "_pytest", "pytest")):
                        module = candidate_module
                        break
                current_frame = current_frame.f_back

            if module is None:
                raise RuntimeError("Cannot determine calling module")

        specs: list[JobSpec] = []
        dependencies: list[type[Dependency]] = []

        for _, obj in inspect.getmembers(module):
            if isinstance(obj, JobSpec):
                specs.append(obj)

            if not inspect.isclass(obj):
                continue

            if issubclass(obj, Dependency) and obj is not Dependency:
                dependencies.append(obj)

        return cls(dependencies=dependencies, specs=specs)
