from collections.abc import Mapping
from typing import Any, Self

from zahir.base_types import (
    Dependency,
    DependencyResult,
    DependencyState,
    propagate_message_override,
    restore_message_override,
    serialise_message_override,
)


class DependencyGroup(Dependency):
    """Await all subdependencies."""

    DEFAULT_MESSAGE: dict[DependencyState, str] = {
        DependencyState.SATISFIED: "All dependencies satisfied.",
        DependencyState.UNSATISFIED: "One or more dependencies not yet satisfied.",
        DependencyState.IMPOSSIBLE: "One or more dependencies are impossible.",
    }

    dependencies: Mapping[str, Dependency | list[Dependency]]

    def __init__(self, dependencies: Mapping[str, Dependency | list[Dependency]]) -> None:
        self.dependencies = dict(dependencies)

    def satisfied(self) -> DependencyResult:
        """Are all subdependencies satisfied?"""

        results = []
        for dependency in self.dependencies.values():
            dep_list = dependency if isinstance(dependency, list) else [dependency]

            # a little expensive, but we need to check all subdependencies now that we collect then for metrics
            for subdep in dep_list:
                result = subdep.satisfied()  # type: ignore[union-attr]
                results.append(result)

        is_impossible = any(result.state == DependencyState.IMPOSSIBLE for result in results)
        is_unsatisfied = any(result.state == DependencyState.UNSATISFIED for result in results)

        if is_impossible:
            state = DependencyState.IMPOSSIBLE
            return DependencyResult(
                type="DependencyGroup",
                state=state,
                message=self.render_message(state, None),
                subdependencies=results,
            )

        if is_unsatisfied:
            state = DependencyState.UNSATISFIED
            return DependencyResult(
                type="DependencyGroup",
                state=state,
                message=self.render_message(state, None),
                subdependencies=results,
            )

        state = DependencyState.SATISFIED
        return DependencyResult(
            type="DependencyGroup",
            state=state,
            message=self.render_message(state, None),
            subdependencies=results,
        )

    def request_extension(self, extra_seconds: float) -> Self:
        """Ask each dependency for a time-extension and return
        the resulting DependencyGroup."""

        result: dict[str, Dependency | list[Dependency]] = {}
        for name, deps in self.dependencies.items():
            if isinstance(deps, list):
                result[name] = [dep.request_extension(extra_seconds) for dep in deps]  # type: ignore[union-attr]
            else:
                result[name] = deps.request_extension(extra_seconds)
        extended = type(self)(result)
        propagate_message_override(self, extended)
        return extended

    def save(self, context) -> Mapping[str, Any]:
        """Save all subdependencies to a dictionary."""

        dependencies = {}
        for name, deps in self.dependencies.items():
            if isinstance(deps, list):
                dependencies[name] = [dep.save(context) for dep in deps]  # type: ignore[union-attr]
            else:
                dependencies[name] = [deps.save(context)]

        result: dict[str, Any] = {"type": "DependencyGroup", "dependencies": dependencies}
        override_data = serialise_message_override(self)
        if override_data:
            result["message_override"] = override_data
        return result

    @classmethod
    def load(cls, context, data: Mapping) -> "DependencyGroup":
        """Load all subdependencies from a dictionary."""

        dependencies = {}

        for name, deps in data["dependencies"].items():
            if isinstance(deps, list):
                deplist: list[Dependency] = []

                for dep_data in deps:
                    dep_class = context.scope.get_dependency_class(dep_data["type"])

                    deplist.append(dep_class.load(context, dep_data))

                dependencies[name] = deplist
            else:
                dep_class = context.scope.get_dependency_class(deps["type"])
                dependencies[name] = dep_class.load(context, deps)

        instance = cls(dependencies)
        restore_message_override(instance, data)
        return instance

    def get(self, name: str) -> Dependency | list[Dependency]:
        """Get a subdependency by name.

        @param name: The name of the subdependency
        @return: The subdependency or list of subdependencies
        """
        value = self.dependencies[name]

        if isinstance(value, list) and len(value) == 1:
            return value[0]

        return value

    def empty(self) -> bool:
        """Check if there are no subdependencies.

        @return: True if there are no subdependencies, False otherwise
        """
        return len(self.dependencies) == 0
