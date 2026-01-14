from collections.abc import Iterator, Mapping
import inspect
from typing import Any, TypeVar

from zahir.base_types import Context, Job
from zahir.context.memory import MemoryContext
from zahir.events import WorkflowOutputEvent, ZahirCustomEvent, ZahirEvent
from zahir.job_registry.sqlite import SQLiteJobRegistry
from zahir.scope import LocalScope
from zahir.worker.overseer import zahir_worker_overseer
from zahir.worker.progress import ProgressMonitor, ZahirProgressMonitor

WorkflowOutputType = TypeVar("WorkflowOutputType", bound=Mapping[str, Any])


DEFAULT_EVENTS: frozenset[type[ZahirEvent]] = frozenset({WorkflowOutputEvent, ZahirCustomEvent})


class LocalWorkflow[WorkflowOutputType]:
    """A workflow execution engine"""

    context: Context | None
    max_workers: int
    progress_monitor: ProgressMonitor | None

    def __init__(
        self,
        context: Context | None = None,
        max_workers: int = 4,
        progress_monitor: ProgressMonitor | None = None,
    ) -> None:
        """Initialize a workflow execution engine

        @param context: The context containing scope, job registry, and event registry
        @param max_workers: Number of worker processes to use
        @param progress_monitor: Optional progress monitor for tracking workflow execution. Defaults to ZahirProgressMonitor if not provided.
        """

        if max_workers < 2:
            raise ValueError("max_workers must be at least 2")

        self.context = context
        self.progress_monitor = progress_monitor

        if not context:
            # find which module called this one, and construct a scope based on the
            # jobs and modules in that librart
            caller_frame = inspect.currentframe()
            caller_module = None

            # Walk up the stack to find a valid module
            # Skip internal zahir modules and pytest modules
            current_frame = caller_frame
            while current_frame is not None:
                candidate_module = inspect.getmodule(current_frame)
                if candidate_module is not None:
                    module_name = candidate_module.__name__
                    # Skip zahir internal modules and pytest modules
                    if not module_name.startswith(("zahir.", "_pytest", "pytest")):
                        caller_module = candidate_module
                        break
                current_frame = current_frame.f_back

            # Make an in-memory job-registry
            scope = LocalScope.from_module(caller_module)
            job_registry = SQLiteJobRegistry(":memory:")
            new_context = MemoryContext(scope=scope, job_registry=job_registry)

            self.context = new_context

        self.max_workers = max_workers

    def run(
        self, start: Job | None = None, events_filter: frozenset[type[ZahirEvent]] | None = DEFAULT_EVENTS
    ) -> Iterator[WorkflowOutputEvent[WorkflowOutputType] | ZahirEvent]:
        """Run all jobs in the registry, opptionally seeding from this job in particular.

        @param start: The starting job of the workflow. Optional; the run will run all pending jobs in the job-register
        @return: Yields an iterator that gives:
        - WorkflowOutputEvent: the actual values yielded from the workflow
        - ZahirCustomEvent: any custom events emitted by workflows. Can be used for custom in-band eventing.
        """

        progress_monitor = self.progress_monitor or ZahirProgressMonitor()
        with progress_monitor as progress:
            for event in zahir_worker_overseer(start, self.context, self.max_workers):
                progress.handle_event(event)

                # Store event in registry if available
                if self.context and hasattr(self.context, "job_registry"):
                    self.context.job_registry.store_event(self.context, event)

                if events_filter is None:
                    yield event
                else:
                    for event_type in events_filter:
                        if isinstance(event, event_type):
                            yield event

    def cancel(self) -> None:
        raise NotImplementedError("LocalWorkflow does not support cancellation at this time.")
