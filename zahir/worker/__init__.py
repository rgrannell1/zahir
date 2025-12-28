from collections.abc import Iterator, Mapping
from typing import Any, Generic, TypeVar

from zahir.base_types import Job
from zahir.events import WorkflowOutputEvent, ZahirCustomEvent
from zahir.worker.overseer import zahir_worker_overseer

WorkflowOutputType = TypeVar("WorkflowOutputType", bound=Mapping[str, Any])


class LocalWorkflow(Generic[WorkflowOutputType]):
    """A workflow execution engine"""

    def __init__(
        self,
        context,
    ) -> None:
        """Initialize a workflow execution engine

        @param context: The context containing scope, job registry, and event registry
        """

        self.context = context
        self.max_workers = 4

    def run(
        self, start: Job | None = None
    ) -> Iterator[WorkflowOutputEvent[WorkflowOutputType] | ZahirCustomEvent]:
        """Run all jobs in the registry, opptionally seeding from this job in particular.

        @param start: The starting job of the workflow. Optional; the run will run all pending jobs in the job-register
        @return: Yields an iterator that gives:
        - WorkflowOutputEvent: the actual values yielded from the workflow
        - ZahirCustomEvent: any custom events emitted by workflows. Can be used for custom in-band eventing.
        """

        if start is not None:
            self.context.job_registry.add(start)

        yield from zahir_worker_overseer(self.context, self.max_workers)


__all__ = ["LocalWorkflow", "zahir_worker_overseer"]
