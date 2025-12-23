"""Dependency implementations for workflow job coordination.

This package provides various dependency types that can be used to coordinate
job execution, including concurrency limits and other resource constraints.
"""

from zahir.dependencies.concurrency import ConcurrencyLimit

__all__ = ["ConcurrencyLimit"]
