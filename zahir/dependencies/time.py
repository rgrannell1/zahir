
from datetime import datetime
from zahir.types import Dependency


class TimeDependency(Dependency):
  after: datetime

  def __init__(self, after: datetime) -> None:
      self.after = after

  def satisfied(self) -> bool:
      """Check whether the time dependency is satisfied."""

      return datetime.now() >= self.after
