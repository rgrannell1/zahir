"""Dependencies

Jobs can depend on things; this implements a few common dependency types. Dependencies
must be serialisable, since we store them alongside jobs.
"""

from zahir.dependencies.group import DependencyGroup
from zahir.dependencies.concurrency import ConcurrencyLimit
from zahir.dependencies.time import TimeDependency
from zahir.dependencies.job import JobDependency


__all__ = ["DependencyGroup", "ConcurrencyLimit", "TimeDependency", "JobDependency"]
