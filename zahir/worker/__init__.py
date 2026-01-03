from collections.abc import Iterator, Mapping
import inspect
from typing import Any, Generic, TypeVar

from zahir.base_types import Context, Job
from zahir.events import WorkflowOutputEvent, ZahirEvent
from zahir.job_registry.sqlite import SQLiteJobRegistry
from zahir.scope import LocalScope
from zahir.worker.overseer import zahir_worker_overseer

WorkflowOutputType = TypeVar("WorkflowOutputType", bound=Mapping[str, Any])


class LocalWorkflow[WorkflowOutputType]:
    """A workflow execution engine"""

    context: Context | None
    max_workers: int

    def __init__(
        self,
        context: Context | None = None,
        max_workers: int = 4,
    ) -> None:
        """Initialize a workflow execution engine

        @param context: The context containing scope, job registry, and event registry
        """

        if max_workers < 2:
            raise ValueError("max_workers must be at least 2")

        self.context = context

        if not context:
            # find which module called this one, and construct a scope based on the
            # jobs and modules in that librart
            caller_frame = inspect.currentframe()
            if caller_frame and caller_frame.f_back:
                caller_module = inspect.getmodule(caller_frame.f_back)
            else:
                caller_module = None

            # Make an in-memory job-registry
            scope = LocalScope.from_module(caller_module)
            job_registry = SQLiteJobRegistry(":memory:")
            new_context = Context(scope=scope, job_registry=job_registry)

            self.context = new_context

        self.max_workers = max_workers

    def run(
        self, start: Job | None = None, all_events: bool = False
    ) -> Iterator[WorkflowOutputEvent[WorkflowOutputType] | ZahirEvent]:
        """Run all jobs in the registry, opptionally seeding from this job in particular.

        @param start: The starting job of the workflow. Optional; the run will run all pending jobs in the job-register
        @return: Yields an iterator that gives:
        - WorkflowOutputEvent: the actual values yielded from the workflow
        - ZahirCustomEvent: any custom events emitted by workflows. Can be used for custom in-band eventing.
        """

        yield from zahir_worker_overseer(start, self.context, self.max_workers, all_events)


__all__ = ["LocalWorkflow", "zahir_worker_overseer"]
