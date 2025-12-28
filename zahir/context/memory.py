from zahir.base_types import Context, EventRegistry, JobRegistry, Scope
from zahir.event_registry.memory import MemoryEventRegistry
from zahir.logging import ZahirLogger


class MemoryContext(Context):
    """A local, direct context implementation. Probably the only one that will be needed."""

    def __init__(
        self,
        scope: Scope,
        job_registry: JobRegistry | None = None,
        event_registry: EventRegistry | None = None,
    ) -> None:
        """Initialize a local context with default registries if not provided

        @param scope: The scope for the context
        @param job_registry: The job registry (defaults to MemoryJobRegistry if None)
        @param event_registry: The event registry (defaults to MemoryEventRegistry if None)
        """

        event_registry = (
            event_registry if event_registry is not None else MemoryEventRegistry()
        )

        job_registry = (
            job_registry if job_registry is not None else MemoryJobRegistry(scope)
        )

        logger = ZahirLogger(event_registry, job_registry)

        super().__init__(
            scope=scope,
            job_registry=job_registry,
            event_registry=event_registry,
            logger=logger,
        )
