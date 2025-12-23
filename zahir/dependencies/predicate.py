

from typing import Callable
from zahir.types import Dependency, DependencyState


class PredicateDependency(Dependency):
  """A dependency that is satisfied based on a predicate function."""

  def __init__(self, predicate: Callable):
    self.predicate = predicate

  def satisfied(self) -> DependencyState:
    return (
        DependencyState.SATISFIED
        if self.predicate()
        else DependencyState.UNSATISFIED
    )
