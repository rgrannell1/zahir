from multiprocessing import Manager
from multiprocessing.queues import Queue
from typing import Any

from zahir.base_types import Context, JobRegistry, Scope
from zahir.utils.id_generator import generate_id


class MemoryContext(Context):
    """A local, direct context implementation. Probably the only one that will be needed."""

    def __init__(self, scope: Scope, job_registry: JobRegistry) -> None:
        """Initialize a local context with default registries if not provided

        @param scope: The scope for the context
        @param job_registry: The job registry
        """

        super().__init__(scope=scope, job_registry=job_registry, manager=(manager := Manager()), state=manager.dict())

    def add_queue(self) -> tuple[str, Queue[Any]]:
        """Create a new queue and store it in context.state.

        @return: A tuple of (queue_id, queue)
        """

        queue_id = generate_id()

        # Manager().Queue() returns a proxy that implements the Queue interface.
        # At runtime, this proxy works like multiprocessing.Queue, but the type
        # checker sees it as queue.Queue[Any]. We use Queue[Any] as the return
        # type since that's what callers expect and the proxy is compatible.

        queue = self.manager.Queue()
        self.state[f"_queue_{queue_id}"] = queue

        # The proxy from Manager().Queue() implements the Queue interface but has
        # a different static type. We know it's compatible at runtime.

        return queue_id, queue  # type: ignore[return-value]

    def get_queue(self, name: str) -> Queue[Any]:
        """Get a queue by ID from context.state.

        @param name: The ID of the queue to retrieve
        @return: The queue
        @raises KeyError: If the queue does not exist
        """

        key = f"_queue_{name}"
        if key not in self.state:
            raise KeyError(f"Queue with ID '{name}' not found")

        # The state dict contains Queue proxies from Manager().Queue(), which
        # are compatible with Queue at runtime.
        return self.state[key]  # type: ignore[return-value]
