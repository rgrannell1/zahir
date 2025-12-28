"""Dependencies

Jobs can depend on things; this implements a few common dependency types. Dependencies
must be serialisable, since we store them alongside jobs.
"""

from zahir.dependencies.concurrency import ConcurrencyLimit
from zahir.dependencies.group import DependencyGroup
from zahir.dependencies.job import JobDependency
from zahir.dependencies.time import TimeDependency

__all__ = ["ConcurrencyLimit", "DependencyGroup", "JobDependency", "TimeDependency"]
