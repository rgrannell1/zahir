"""Context type.

Contains the Context dataclass which bundles scope, job registry,
and shared state together for passing to jobs and dependencies.
"""

from abc import abstractmethod
from dataclasses import dataclass
from multiprocessing.managers import DictProxy, SyncManager
from multiprocessing.queues import Queue as MPQueue
from typing import Any

from zahir.types.registry import JobRegistry
from zahir.types.scope import Scope


@dataclass
class Context:
    """Context such as scope, the job registry, and event registry
    needs to be communicated to Jobs and Dependencies.
    """

    # The scope containing registered job and dependency classes
    scope: Scope
    # Keep track of jobs
    job_registry: JobRegistry
    # Shared state creator
    manager: SyncManager
    # Shared state dictionary. Used for storing semaphores (concurrencylimit)
    state: DictProxy[str, Any]

    @abstractmethod
    def get_queue(self, name: str) -> MPQueue[Any]:
        """Get a named queue from the context."""

        raise NotImplementedError
