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
        self._scope = scope
        self._job_registry = job_registry
        self._manager: Manager | None = None
        self._state: multiprocessing.managers.DictProxy | None = None
    
    @property
    def scope(self) -> Scope:
        return self._scope
    
    @property
    def job_registry(self) -> JobRegistry:
        return self._job_registry
    
    @property
    def manager(self) -> Manager:
        """Lazy initialization of Manager to avoid issues during unpickling."""
        if self._manager is None:
            self._manager = Manager()
            self._state = self._manager.dict()
        return self._manager
    
    @property
    def state(self) -> multiprocessing.managers.DictProxy:
        """Lazy initialization of state dict."""
        if self._state is None:
            # Trigger manager creation
            _ = self.manager
        assert self._state is not None
        return self._state

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
    
    def __getstate__(self) -> dict:
        """Custom pickling: don't pickle the Manager, reconstruct it on unpickle."""
        return {
            "_scope": self._scope,
            "_job_registry": self._job_registry,
            # Don't pickle manager or state - they'll be reconstructed lazily
        }
    
    def __setstate__(self, state: dict) -> None:
        """Custom unpickling: restore scope and registry, Manager will be created lazily."""
        self._scope = state["_scope"]
        self._job_registry = state["_job_registry"]
        # Don't create Manager here - it will be created lazily on first access
        self._manager = None
        self._state = None
