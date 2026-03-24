import atexit
import logging
from multiprocessing.queues import Queue
import os
import pathlib
import time

from zahir.base_types import Context, DependencyState, JobInstance, JobState
from zahir.constants import DEPENDENCY_LOOP_STALL_SECONDS
from zahir.events import (
    JobReadyEvent,
    WorkflowCompleteEvent,
    ZahirInternalErrorEvent,
)
from zahir.exception import ImpossibleDependencyError, exception_to_text_blob
from zahir.otel.metrics import MetricsCollector
from zahir.otel.metrics_exporter import FileMetricsExporter
from zahir.serialise import SerialisedEvent, serialise_event
from zahir.types.dependency import DependencyResult
from zahir.utils.logging_config import configure_logging
from zahir.utils.output_logging import setup_output_logging

log = logging.getLogger(__name__)

type OutputQueue = Queue[SerialisedEvent]


def setup_dependency_worker(
    context: Context,
    log_dir: str | None = None,
) -> None:
    """Configure logging and output logging for the dependency worker."""

    configure_logging()

    if log_dir:
        setup_output_logging(log_dir=log_dir)


def _setup_job_registry(context: Context) -> None:
    """Initialize the job registry and register cleanup on exit."""

    job_registry = context.job_registry
    job_registry.init(str(os.getpid()))
    atexit.register(job_registry.close)


def workflow_completed(
    job_registry,
    workflow_id: str,
) -> bool:
    """
    Check if the workflow is complete and handle completion if so.

    Returns:
        True if workflow is complete and worker should exit, False otherwise.
    """
    current_workflow_complete = not job_registry.is_active(workflow_id=workflow_id)
    active_workflow_ids = job_registry.get_active_workflow_ids()

    return current_workflow_complete and not active_workflow_ids


def create_impossible_error(job: JobInstance, dependencies_result: DependencyResult) -> ImpossibleDependencyError:
    """Create an ImpossibleDependencyError from a DependencyResult."""

    return ImpossibleDependencyError(f"Job {job.job_id} has impossible dependencies.")


def job_dependencies_satisfied(
    context: Context,
    output_queue: OutputQueue,
    job_registry,
    job,
    workflow_id: str,
) -> bool:
    """
    Process a single pending job by checking its dependencies.

    Returns:
        True if the job became ready, False otherwise.
    """
    dependencies_result = job.dependencies.satisfied()

    if dependencies_result.state == DependencyState.SATISFIED:
        job_registry.set_state(
            context, job.job_id, job.spec.type, workflow_id, output_queue, JobState.READY, recovery=False
        )
        return True
    if dependencies_result.state == DependencyState.IMPOSSIBLE:
        error = create_impossible_error(job, dependencies_result)
        job_registry.set_state(
            context,
            job.job_id,
            job.spec.type,
            workflow_id,
            output_queue,
            JobState.IMPOSSIBLE,
            error=error,
            recovery=False,
        )
    return False


def has_ready_jobs(context: Context, job_registry, workflow_id: str) -> bool:
    """Check if there are any READY jobs in the workflow."""
    for job_info in job_registry.jobs(context, state=JobState.READY, workflow_id=workflow_id, limit=1):
        return True

    return False


def has_active_jobs(
    context: Context,
    output_queue: OutputQueue,
    job_registry,
    workflow_id: str,
) -> bool:
    """
    Process all jobs for a specific workflow.

    Returns:
        True if any jobs became ready, False otherwise.
    """
    any_ready = False

    # Process pending jobs
    for job_info in job_registry.jobs(context, state=JobState.PENDING, workflow_id=workflow_id):
        job = job_info.job
        if job_dependencies_satisfied(context, output_queue, job_registry, job, workflow_id):
            any_ready = True

    # Check for existing ready jobs
    if has_ready_jobs(context, job_registry, workflow_id):
        any_ready = True

    return any_ready


def has_active_workflows(
    context: Context,
    output_queue: OutputQueue,
    job_registry,
    workflow_id: str,
    active_workflow_ids: list[str],
) -> bool:
    """
    Process jobs from all active workflows.

    Returns:
        True if any jobs became ready
    """
    workflows = {workflow_id} | set(active_workflow_ids)
    return any(has_active_jobs(context, output_queue, job_registry, workflow) for workflow in workflows)


def handle_exception(
    context: Context,
    output_queue: OutputQueue,
    workflow_id: str,
    err: Exception,
) -> None:
    """Sent error event on error."""

    output_queue.put(
        serialise_event(
            context,
            ZahirInternalErrorEvent(
                workflow_id=workflow_id,
                error=exception_to_text_blob(err),
            ),
        )
    )


# ── Instrumented variants for metrics collection ─────────────────


def has_active_workflows_instrumented(
    context: Context,
    output_queue: OutputQueue,
    job_registry,
    workflow_id: str,
    active_workflow_ids: list[str],
) -> tuple[int, int, int]:
    """Like ``has_active_workflows`` but returns (pending_checked, jobs_made_ready, get_state_calls).

    This is used by the instrumented main loop so we can record per-iteration
    metrics without altering the original helper signatures.
    """
    workflows = {workflow_id} | set(active_workflow_ids)
    total_pending = 0
    total_ready = 0
    total_state_calls = 0

    for workflow in workflows:
        pending, ready, state_calls = has_active_jobs_instrumented(
            context, output_queue, job_registry, workflow,
        )
        total_pending += pending
        total_ready += ready
        total_state_calls += state_calls

    return total_pending, total_ready, total_state_calls


def has_active_jobs_instrumented(
    context: Context,
    output_queue: OutputQueue,
    job_registry,
    workflow_id: str,
) -> tuple[int, int, int]:
    """Like ``has_active_jobs`` but returns (pending_checked, jobs_made_ready, get_state_calls).

    ``get_state_calls`` is an *estimate*: we count one call per dependency per
    pending job (since ``JobCompletedDependency.satisfied()`` calls
    ``get_state()`` for each dependency).
    """
    pending_checked = 0
    jobs_made_ready = 0
    state_calls = 0

    for job_info in job_registry.jobs(context, state=JobState.PENDING, workflow_id=workflow_id):
        job = job_info.job
        pending_checked += 1
        # Each dependency.satisfied() call triggers one get_state() per dep.
        state_calls += len(job.dependencies) if hasattr(job.dependencies, "__len__") else 1
        if job_dependencies_satisfied(context, output_queue, job_registry, job, workflow_id):
            jobs_made_ready += 1

    return pending_checked, jobs_made_ready, state_calls


def _any_existing_ready(
    context: Context,
    job_registry,
    workflow_id: str,
    active_workflow_ids: list[str],
) -> bool:
    """Check whether any active workflow has READY jobs already queued."""
    workflows = {workflow_id} | set(active_workflow_ids)
    return any(has_ready_jobs(context, job_registry, workflow) for workflow in workflows)


def _create_dep_worker_instruments(metrics: MetricsCollector) -> dict[str, object]:
    """Create all metric instruments for the dependency worker loop.

    Returns a dict of instrument name to instrument so the main loop
    can reference them without burning 6+ local variables.
    """
    return {
        "loop_duration": metrics.get_histogram(
            "zahir.dep_worker.loop_duration_ms",
            description="Time per dependency-check loop iteration",
            unit="ms",
        ),
        "pending_checked": metrics.get_histogram(
            "zahir.dep_worker.pending_jobs_checked",
            description="Pending jobs examined per loop iteration",
            unit="{jobs}",
            bounds=[0, 1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500],
        ),
        "jobs_made_ready": metrics.get_gauge(
            "zahir.dep_worker.jobs_made_ready",
            description="Jobs transitioned PENDING to READY this iteration",
            unit="{jobs}",
        ),
        "active_workflows": metrics.get_gauge(
            "zahir.dep_worker.active_workflows",
            description="Number of active workflows",
            unit="{workflows}",
        ),
        "get_state_calls": metrics.get_histogram(
            "zahir.dep_worker.get_state_calls",
            description="get_state() calls per loop iteration",
            unit="{calls}",
            bounds=[0, 1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000],
        ),
    }


def zahir_dependency_worker(
    context: Context,
    output_queue: OutputQueue,
    workflow_id: str,
    log_dir: str | None = None,
    otel_output_dir: str | None = None,
) -> None:
    """Analyse job dependencies and mark jobs as pending."""
    setup_dependency_worker(context, log_dir=log_dir)

    # Set up metrics collection (written alongside traces).
    metrics = MetricsCollector()
    metrics_exporter: FileMetricsExporter | None = None
    if otel_output_dir is not None:
        metrics_dir = pathlib.Path(otel_output_dir)
        metrics_dir.mkdir(parents=True, exist_ok=True)
        metrics_exporter = FileMetricsExporter(metrics_dir / f"metrics-dep-worker-{os.getpid()}.jsonl")

    instruments = _create_dep_worker_instruments(metrics)

    try:
        _setup_job_registry(context)
        job_registry = context.job_registry

        while True:
            loop_start = time.monotonic()

            if workflow_completed(job_registry, workflow_id):
                duration = job_registry.get_workflow_duration(workflow_id=workflow_id) or 0.0
                output_queue.put(
                    serialise_event(
                        context,
                        WorkflowCompleteEvent(
                            workflow_id=workflow_id,
                            duration_seconds=duration,
                        ),
                    )
                )
                # Final flush before exit.
                if metrics_exporter:
                    metrics.flush(metrics_exporter)
                return

            active_workflow_ids = job_registry.get_active_workflow_ids()
            instruments["active_workflows"].set(len(active_workflow_ids) + 1)

            # Count pending jobs and ready transitions this iteration.
            pending_count, ready_count, state_calls = has_active_workflows_instrumented(
                context, output_queue, job_registry, workflow_id, active_workflow_ids,
            )
            instruments["pending_checked"].record(pending_count)
            instruments["jobs_made_ready"].set(ready_count)
            instruments["get_state_calls"].record(state_calls)

            any_ready = ready_count > 0 or _any_existing_ready(context, job_registry, workflow_id, active_workflow_ids)

            if any_ready:
                output_queue.put(serialise_event(context, JobReadyEvent()))

            loop_elapsed_ms = (time.monotonic() - loop_start) * 1000
            instruments["loop_duration"].record(loop_elapsed_ms)

            # Flush metrics every loop so the file is always up-to-date.
            if metrics_exporter:
                metrics.flush(metrics_exporter)

            time.sleep(DEPENDENCY_LOOP_STALL_SECONDS)

    except Exception as err:
        handle_exception(context, output_queue, workflow_id, err)
    finally:
        if metrics_exporter:
            metrics_exporter.close()
