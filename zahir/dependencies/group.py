from typing import Mapping

from zahir.base_types import Dependency, DependencyState


class DependencyGroup(Dependency):
    """Await a all subdependencies."""

    dependencies: Mapping[str, Dependency | list[Dependency]]

    def __init__(
        self, dependencies: Mapping[str, Dependency | list[Dependency]]
    ) -> None:
        self.dependencies = dict(dependencies)

    def satisfied(self) -> DependencyState:
        """Are all subdependencies satisfied?"""

        for dependency in self.dependencies.values():
            dep_list = dependency if isinstance(dependency, list) else [dependency]

            for subdep in dep_list:
                state = subdep.satisfied()  # type: ignore[union-attr]

                if state == DependencyState.UNSATISFIED:
                    return DependencyState.UNSATISFIED
                elif state == DependencyState.IMPOSSIBLE:
                    return DependencyState.IMPOSSIBLE

        return DependencyState.SATISFIED

    def save(self) -> Mapping:
        """Save all subdependencies to a dictionary."""

        dependencies = {}
        for name, deps in self.dependencies.items():
            if isinstance(deps, list):
                dependencies[name] = [dep.save() for dep in deps]  # type: ignore[union-attr]
            else:
                dependencies[name] = [deps.save()]

        return {"type": "DependencyGroup", "dependencies": dependencies}

    @classmethod
    def load(cls, context, data: Mapping) -> "DependencyGroup":
        """Load all subdependencies from a dictionary."""

        dependencies = {}

        for name, deps in data["dependencies"].items():
            if isinstance(deps, list):
                deplist: list[Dependency] = []

                for dep_data in deps:
                    DepClass = context.scope.get_dependency_class(dep_data["type"])

                    deplist.append(DepClass.load(context, dep_data))

                dependencies[name] = deplist
            else:
                DepClass = context.scope.get_dependency_class(deps["type"])
                dependencies[name] = DepClass.load(context, deps)

        return cls(dependencies)

    def get(self, name: str) -> Dependency | list[Dependency]:
        """Get a subdependency by name.

        @param name: The name of the subdependency
        @return: The subdependency or list of subdependencies
        """
        return self.dependencies[name]

    def empty(self) -> bool:
        """Check if there are no subdependencies.

        @return: True if there are no subdependencies, False otherwise
        """
        return len(self.dependencies) == 0
