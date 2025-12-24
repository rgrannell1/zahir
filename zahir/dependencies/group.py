


from zahir.types import Dependency, DependencyState


class DependencyGroup(Dependency):
  """Await a list of dependencies to be satisfied."""

  dependencies: list[Dependency]

  def __init__(self, dependencies: list[Dependency]) -> None:
      self.dependencies = dependencies

  def satisfied(self) -> DependencyState:
    for dependency in self.dependencies:
      state = dependency.satisfied()

      if state == DependencyState.UNSATISFIED:
        return DependencyState.UNSATISFIED
      elif state == DependencyState.IMPOSSIBLE:
        return DependencyState.IMPOSSIBLE

    return DependencyState.SATISFIED

  def save(self) -> dict:
    return {
      "dependencies": [dependency.save() for dependency in self.dependencies],
    }

  @classmethod
  def load(cls, data: dict) -> "DependencyGroup":
    # we need to determine the correct subclass to call load on, which
    # will require scope-lookup.

    raise NotImplementedError("Loading DependencyGroup is not implemented yet.")
