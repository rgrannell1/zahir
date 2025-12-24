from typing import Mapping, TypeVar, cast

from zahir.types import Dependency, DependencyState

_DT = TypeVar("_DT", bound=Dependency)


class DependencyGroup(Dependency):
    """Await a all subdependencies."""

    dependencies: dict[str, Dependency | list[Dependency]]

    def __init__(self, dependencies: Mapping[str, _DT | list[_DT]]) -> None:
        # Cast is safe because _DT is bound to Dependency
        self.dependencies = cast(
            dict[str, Dependency | list[Dependency]], dict(dependencies)
        )

    def satisfied(self) -> DependencyState:
        """Are all subdependencies satisfied?"""

        for dependency in self.dependencies.values():
            dep_list = dependency if isinstance(dependency, list) else [dependency]

            for subdep in dep_list:
                state = subdep.satisfied()

                if state == DependencyState.UNSATISFIED:
                    return DependencyState.UNSATISFIED
                elif state == DependencyState.IMPOSSIBLE:
                    return DependencyState.IMPOSSIBLE

        return DependencyState.SATISFIED

    def save(self) -> dict:
        """Save all subdependencies to a dictionary."""

        dependencies = {}
        for name, deps in self.dependencies.items():
            if isinstance(deps, list):
                dependencies[name] = [dep.save() for dep in deps]
            else:
                dependencies[name] = [deps.save()]

        return {"type": "DependencyGroup", "dependencies": dependencies}

    @classmethod
    def load(cls, context, data: dict) -> "DependencyGroup":
        """Load all subdependencies from a dictionary."""

        dependencies = {}

        for name, deps in data["dependencies"].items():
            if isinstance(deps, list):
                dependencies[name] = [
                    context.scope.get_dependency_class(dep_data) for dep_data in deps
                ]
            else:
                dependencies[name] = context.scope.get_dependency_class(deps)

        return cls(dependencies)
