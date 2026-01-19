"""Job registry implementations for workflow execution.

This package provides various job registry implementations for managing job
execution in workflows.
"""

from zahir.jobs.decorator import spec
from zahir.jobs.empty import Empty
from zahir.jobs.sleep import Sleep

__all__ = ["Empty", "Sleep", "spec"]
