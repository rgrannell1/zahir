from collections.abc import Iterator, Mapping
from typing import Any, TypeVar

from zahir.base_types import Context, JobInstance
from zahir.context.memory import MemoryContext
from zahir.events import WorkflowOutputEvent, ZahirCustomEvent, ZahirEvent
from zahir.job_registry.sqlite import SQLiteJobRegistry
from zahir.scope import LocalScope, _find_caller_module
from zahir.worker.overseer import zahir_worker_overseer
from zahir.worker.progress import NoOpProgressMonitor, ProgressMonitor, ZahirProgressMonitor

WorkflowOutputType = TypeVar("WorkflowOutputType", bound=Mapping[str, Any])


DEFAULT_EVENTS: frozenset[type[ZahirEvent]] = frozenset({WorkflowOutputEvent, ZahirCustomEvent})


class LocalWorkflow[WorkflowOutputType]:
    """A workflow execution engine"""

    context: Context | None
    max_workers: int
    progress_monitor: ProgressMonitor | None
    log_output_dir: str | None
    start_job_type: str | None
    otel_output_dir: str | None

    def __init__(
        self,
        context: Context | None = None,
        max_workers: int = 4,
        progress_monitor: ProgressMonitor | None = None,
        log_output_dir: str | None = None,
        start_job_type: str | None = None,
        otel_output_dir: str | None = "traces",
    ) -> None:
        """Initialize a workflow execution engine

        @param context: The context containing scope, job registry, and event registry
        @param max_workers: Number of worker processes to use
        @param progress_monitor: Optional progress monitor for tracking workflow execution. Defaults to ZahirProgressMonitor if not provided.
        @param log_output_dir: Directory to capture stdout/stderr from all processes. Defaults to "zahir_logs".
        @param otel_output_dir: Directory to write OpenTelemetry trace files. Defaults to "traces". Pass None explicitly to disable tracing.
        """

        if max_workers < 2:
            raise ValueError("max_workers must be at least 2")

        self.context = context
        self.progress_monitor = progress_monitor
        self.log_output_dir = log_output_dir if log_output_dir is not None else "zahir_logs"
        self.start_job_type = start_job_type
        self.otel_output_dir = otel_output_dir

        if not context:
            # Find which module called this one, and construct a scope based on the
            # jobs and modules in that library
            caller_module = _find_caller_module()

            # Make an in-memory job-registry
            scope = LocalScope.from_module(caller_module)
            job_registry = SQLiteJobRegistry(":memory:")
            context = MemoryContext(scope=scope, job_registry=job_registry)

        self.context = context

        self.max_workers = max_workers

    def run(
        self,
        start: JobInstance | None = None,
        events_filter: frozenset[type[ZahirEvent]] | None = DEFAULT_EVENTS,
        show_progress: bool = True,
    ) -> Iterator[WorkflowOutputEvent[WorkflowOutputType] | ZahirEvent]:
        """Run all jobs in the registry, optionally seeding from this job in particular.

        @param start: The starting job of the workflow. Optional; the run will run all pending jobs in the job-register
        @param events_filter: Filter which events to yield. Defaults to WorkflowOutputEvent and ZahirCustomEvent.
        @param show_progress: Whether to show the progress monitor. Defaults to True.
        @return: Yields an iterator that gives:
        - WorkflowOutputEvent: the actual values yielded from the workflow
        - ZahirCustomEvent: any custom events emitted by workflows. Can be used for custom in-band eventing.
        """

        progress_monitor = self.progress_monitor or ZahirProgressMonitor() if show_progress else NoOpProgressMonitor()

        with progress_monitor as progress:
            for event in zahir_worker_overseer(
                start,
                self.context,
                self.max_workers,
                self.otel_output_dir,
                log_output_dir=self.log_output_dir,
                start_job_type=self.start_job_type,
            ):
                progress.handle_event(event)

                if events_filter is None:
                    yield event
                else:
                    for event_type in events_filter:
                        if isinstance(event, event_type):
                            yield event

    def cancel(self) -> None:
        raise NotImplementedError("LocalWorkflow does not support cancellation at this time.")
