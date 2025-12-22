"""Dependency implementations for workflow task coordination.

This package provides various dependency types that can be used to coordinate
task execution, including concurrency limits and other resource constraints.
"""

from zahir.dependencies.concurrency import ConcurrencyLimit

__all__ = ["ConcurrencyLimit"]
