from zahir.base_types import Context, EventRegistry, JobRegistry, Scope
from zahir.logging import ZahirLogger


class MemoryContext(Context):
    """A local, direct context implementation. Probably the only one that will be needed."""

    def __init__(
        self,
        scope: Scope,
        job_registry: JobRegistry
    ) -> None:
        """Initialize a local context with default registries if not provided

        @param scope: The scope for the context
        @param job_registry: The job registry
        """

        super().__init__(
            scope=scope,
            job_registry=job_registry,
            logger=ZahirLogger(job_registry)
        )
