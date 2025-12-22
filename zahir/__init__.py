"""Zahir - A parallel workflow execution engine with dependency management.

Zahir provides a flexible framework for defining and executing workflows composed
of interdependent tasks. It supports parallel execution, dependency management,
concurrency limiting, and error recovery.

Main Components:
    - Task: Abstract base class for defining workflow tasks
    - Workflow: Execution engine for running task workflows
    - Dependency: Abstract base class for task dependencies
    - JobQueue: Interface for managing task execution queues

Example:
    >>> from zahir import Workflow, Task
    >>> workflow = Workflow(max_workers=4)
    >>> workflow.run(start_task)
"""

from zahir.types import Task, Dependency, JobQueue
from zahir.workflow import Workflow
from zahir.queues.local import MemoryJobQueue
from zahir.dependencies.concurrency import ConcurrencyLimit

__version__ = "0.1.0"

__all__ = [
    "Task",
    "Dependency",
    "JobQueue",
    "Workflow",
    "MemoryJobQueue",
    "ConcurrencyLimit",
]
