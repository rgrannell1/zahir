"""Map Zahir events to OpenTelemetry span operations.

Each public function receives a `TraceContextManager` and the relevant event,
then creates / updates / finalises spans accordingly.
"""

from __future__ import annotations

from datetime import UTC, datetime
import json
import logging
from typing import TYPE_CHECKING, Any

from zahir.events import (
    JobCompletedEvent,
    JobImpossibleEvent,
    JobIrrecoverableEvent,
    JobOutputEvent,
    JobPausedEvent,
    JobPrecheckFailedEvent,
    JobRecoveryCompletedEvent,
    JobRecoveryStartedEvent,
    JobRecoveryTimeoutEvent,
    JobStartedEvent,
    JobTimeoutEvent,
    JobWorkerWaitingEvent,
    WorkflowCompleteEvent,
    WorkflowStartedEvent,
)
from zahir.exception import exception_from_text_blob
from zahir.otel.spans import STATUS_ERROR, STATUS_OK, add_attribute, finalise_span, new_span

if TYPE_CHECKING:
    from zahir.otel.trace_manager import TraceContextManager

log = logging.getLogger(__name__)


# ── Workflow Events ─────────────────────────────────────────────────


def handle_workflow_started(manager: TraceContextManager, event: WorkflowStartedEvent) -> None:
    """Start the root workflow span and record its trace ID."""
    trace_id = manager.register_workflow(event.workflow_id)

    workflow_inputs = manager.collect_workflow_inputs(event.workflow_id)

    attributes: dict[str, Any] = {
        "workflow.id": event.workflow_id,
        "workflow.pid": event.pid,
        "process.pid": event.pid,
    }

    for key, value in workflow_inputs.items():
        if isinstance(value, (str, int, float, bool)):
            attributes[f"workflow.input.{key}"] = value
        else:
            attributes[f"workflow.input.{key}"] = json.dumps(value)[:500]

    span = new_span(
        trace_id=trace_id,
        name=f"workflow:{event.workflow_id}",
        start_time=datetime.now(UTC),
        attributes=attributes,
    )
    manager.store_span(f"_workflow:{event.workflow_id}", span)


def handle_workflow_complete(manager: TraceContextManager, event: WorkflowCompleteEvent) -> None:
    """Finalise the root workflow span and all associated process spans."""
    workflow_key = f"_workflow:{event.workflow_id}"
    span = manager.get_span(workflow_key)
    if span is None:
        return

    now = datetime.now(UTC)
    finalise_span(span, now, STATUS_OK)
    add_attribute(span, "workflow.duration_seconds", event.duration_seconds)
    manager.export_span(event.workflow_id, span)
    manager.remove_span(workflow_key)

    manager.end_all_process_spans(event.workflow_id, now)


# ── Worker Events ───────────────────────────────────────────────────


def handle_job_worker_waiting(manager: TraceContextManager, event: JobWorkerWaitingEvent) -> None:
    """Record a worker-ready signal as a tiny child span on its process span."""
    workflow_id = event.workflow_id or manager.workflow_for_process(event.pid)
    if workflow_id is None:
        return

    manager.associate_process(event.pid, workflow_id)

    trace_id = manager.trace_id_for(workflow_id)
    if trace_id is None:
        return

    process_span_id = manager.ensure_process_span(event.pid, workflow_id, datetime.now(UTC))
    if process_span_id is None:
        return

    now = datetime.now(UTC)
    ready_span = new_span(
        trace_id=trace_id,
        name=f"worker-ready-{event.pid}",
        start_time=now,
        parent_span_id=process_span_id,
        attributes={
            "worker.pid": event.pid,
            "worker.ready_signal": True,
        },
    )
    finalise_span(ready_span, now, STATUS_OK)
    manager.export_span(workflow_id, ready_span)


# ── Job Lifecycle Events ───────────────────────────────────────────


def handle_job_started(manager: TraceContextManager, event: JobStartedEvent) -> None:
    """Open a span for a newly started job."""
    trace_id = manager.trace_id_for(event.workflow_id)
    if trace_id is None:
        return

    now = datetime.now(UTC)

    # Parent is the process span, falling back to the workflow span
    parent_span_id = manager.ensure_process_span(event.pid, event.workflow_id, now)
    if parent_span_id is None:
        workflow_span = manager.get_span(f"_workflow:{event.workflow_id}")
        parent_span_id = workflow_span["spanId"] if workflow_span else None
    if parent_span_id is None:
        log.warning("No parent span for job %s (pid=%s, workflow=%s)", event.job_id, event.pid, event.workflow_id)
        return

    attributes: dict[str, Any] = {
        "job.id": event.job_id,
        "job.type": event.job_type,
        "job.workflow_id": event.workflow_id,
        "job.worker_pid": event.pid,
        "process.pid": event.pid,
    }

    _add_dispatch_timing(manager, event.job_id, now, attributes)
    _add_job_input_attributes(manager, event.job_id, attributes)

    links = _build_parent_job_links(manager, event.job_id, attributes)

    span = new_span(
        trace_id=trace_id,
        name=f"job:{event.job_type}",
        start_time=now,
        parent_span_id=parent_span_id,
        attributes=attributes,
        links=links,
    )
    manager.store_span(event.job_id, span)


def handle_job_completed(manager: TraceContextManager, event: JobCompletedEvent) -> None:
    """Finalise a successfully completed job span."""
    manager.end_span(event.job_id, datetime.now(UTC), STATUS_OK)


def handle_job_timeout(manager: TraceContextManager, event: JobTimeoutEvent) -> None:
    """Finalise a timed-out job span."""
    span = manager.get_span(event.job_id)
    if span is not None:
        add_attribute(span, "job.timeout_seconds", event.duration_seconds)
    manager.end_span(event.job_id, datetime.now(UTC), STATUS_ERROR, f"Job timed out after {event.duration_seconds}s")


def handle_job_irrecoverable(manager: TraceContextManager, event: JobIrrecoverableEvent) -> None:
    """Finalise a job that failed irrecoverably."""
    try:
        exc = exception_from_text_blob(event.error)
        error_msg = str(exc)
        error_type = type(exc).__name__
    except Exception:
        error_msg = event.error
        error_type = "Unknown"

    span = manager.get_span(event.job_id)
    if span is not None:
        add_attribute(span, "job.error", error_msg)
        add_attribute(span, "job.error_type", error_type)
    manager.end_span(event.job_id, datetime.now(UTC), STATUS_ERROR, f"Job failed irrecoverably: {error_msg}")


def handle_job_precheck_failed(manager: TraceContextManager, event: JobPrecheckFailedEvent) -> None:
    """Finalise a job whose precheck failed."""
    span = manager.get_span(event.job_id)
    if span is not None:
        add_attribute(span, "job.precheck_error", event.error)
    manager.end_span(event.job_id, datetime.now(UTC), STATUS_ERROR, f"Job precheck failed: {event.error}")


def handle_job_paused(manager: TraceContextManager, event: JobPausedEvent) -> None:
    """Mark a job as paused (span stays open)."""
    span = manager.get_span(event.job_id)
    if span is not None:
        add_attribute(span, "job.paused", True)


def handle_job_impossible(manager: TraceContextManager, event: JobImpossibleEvent) -> None:
    """Finalise a job marked as impossible."""
    span = manager.get_span(event.job_id)
    if span is not None:
        add_attribute(span, "job.impossible", True)
    manager.end_span(event.job_id, datetime.now(UTC), STATUS_ERROR, "Job marked as impossible")


def handle_job_output(manager: TraceContextManager, event: JobOutputEvent) -> None:
    """Attach output summary to a running job span."""
    job_id = event.job_id or ""
    span = manager.get_span(job_id)
    if span is not None and event.output:
        add_attribute(span, "job.output_summary", json.dumps(event.output)[:500])


# ── Recovery Events ────────────────────────────────────────────────


def handle_job_recovery_started(manager: TraceContextManager, event: JobRecoveryStartedEvent) -> None:
    """Open a recovery sub-span beneath the original job span."""
    trace_id = manager.trace_id_for(event.workflow_id)
    if trace_id is None:
        return

    job_span = manager.get_span(event.job_id)
    parent_span_id = job_span["spanId"] if job_span else None
    if parent_span_id is None:
        return

    span = new_span(
        trace_id=trace_id,
        name=f"job.recovery:{event.job_type}",
        start_time=datetime.now(UTC),
        parent_span_id=parent_span_id,
        attributes={
            "job.id": event.job_id,
            "job.type": event.job_type,
            "job.recovery": True,
        },
    )
    manager.store_span(f"{event.job_id}:recovery", span)


def handle_job_recovery_completed(manager: TraceContextManager, event: JobRecoveryCompletedEvent) -> None:
    """Finalise a recovery span as successful."""
    manager.end_span(f"{event.job_id}:recovery", datetime.now(UTC), STATUS_OK)


def handle_job_recovery_timeout(manager: TraceContextManager, event: JobRecoveryTimeoutEvent) -> None:
    """Finalise a timed-out recovery span."""
    recovery_key = f"{event.job_id}:recovery"
    span = manager.get_span(recovery_key)
    if span is not None:
        add_attribute(span, "job.recovery.timeout_seconds", event.duration_seconds)
    manager.end_span(recovery_key, datetime.now(UTC), STATUS_ERROR, "Recovery timed out")


# ── Private Helpers ────────────────────────────────────────────────


def _add_dispatch_timing(manager: TraceContextManager, job_id: str, now: datetime, attributes: dict[str, Any]) -> None:
    """Try to add dispatch-delay metrics to *attributes*."""
    try:
        job_timing = manager.context.job_registry.get_job_timing(job_id)
        if job_timing and job_timing.started_at:
            delay_ms = (now - job_timing.started_at).total_seconds() * 1000
            attributes["job.dispatch_delay_ms"] = round(delay_ms, 2)
            attributes["job.dispatched_at"] = job_timing.started_at.isoformat()
    except Exception as err:
        log.debug("Could not get job timing for %s: %s", job_id, err)


def _add_job_input_attributes(manager: TraceContextManager, job_id: str, attributes: dict[str, Any]) -> None:
    """Try to serialise job inputs into *attributes*."""
    job = manager.job_from_registry(job_id)
    if job is None:
        return

    try:
        job_input = job.input
        if isinstance(job_input, dict):
            for key, value in job_input.items():
                if isinstance(value, (str, int, float, bool)):
                    attributes[f"job.input.{key}"] = value
                else:
                    attributes[f"job.input.{key}"] = json.dumps(value)[:500]
        elif job_input is not None:
            attributes["job.input"] = json.dumps(job_input)[:500]
    except Exception as err:
        log.debug("Could not serialise job inputs for %s: %s", job_id, err)


def _build_parent_job_links(
    manager: TraceContextManager,
    job_id: str,
    attributes: dict[str, Any],
) -> list[dict[str, Any]] | None:
    """If *job_id* has a parent job, add a link and attribute for it."""
    parent_job_id = manager.parent_job_id(job_id)
    if parent_job_id is None:
        return None

    attributes["job.parent_id"] = parent_job_id

    parent_span = manager.get_span(parent_job_id)
    if parent_span is None:
        return None

    return [
        {
            "traceId": parent_span["traceId"],
            "spanId": parent_span["spanId"],
            "attributes": [
                {"key": "job.dependency.type", "value": {"stringValue": "awaited_by"}},
                {"key": "job.parent.id", "value": {"stringValue": parent_job_id}},
            ],
        }
    ]
