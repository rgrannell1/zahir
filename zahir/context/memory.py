import multiprocessing
from multiprocessing import Manager

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

    def add_queue(self) -> tuple[str, multiprocessing.Queue]:
        """Create a new queue and store it in context.state.

        @return: A tuple of (queue_id, queue)
        """
        queue_id = generate_id()
        queue = self.manager.Queue()
        self.state[f"_queue_{queue_id}"] = queue
        return queue_id, queue

    def get_queue(self, queue_id: str) -> multiprocessing.Queue:
        """Get a queue by ID from context.state.

        @param queue_id: The ID of the queue to retrieve
        @return: The queue
        @raises KeyError: If the queue does not exist
        """
        key = f"_queue_{queue_id}"
        if key not in self.state:
            raise KeyError(f"Queue with ID '{queue_id}' not found")
        return self.state[key]
