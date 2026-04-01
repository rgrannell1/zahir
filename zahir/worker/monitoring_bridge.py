"""Bridge between ZahirEvents and the MonitoringEmitter.

Responsible for enriching events with registry data before emission.
This keeps all monitoring-related I/O out of both the core event types
and the overseer dispatch loop.
"""

from __future__ import annotations

from datetime import UTC, datetime
import json
import logging
from typing import TYPE_CHECKING, Any

from zahir.events import JobStartedEvent, WorkflowStartedEvent, ZahirEvent

if TYPE_CHECKING:
    from zahir.base_types import Context
    from zahir.monitoring.emitter import MonitoringEmitter

log = logging.getLogger(__name__)


def enrich_and_emit(event: ZahirEvent, context: Context, emitter: MonitoringEmitter) -> None:
    """Enrich *event* with registry data, then emit it."""
    _enrich(event, context)
    emitter.emit(event)


# ── Enrichment ────────────────────────────────────────────────────────────────


def _enrich(event: ZahirEvent, context: Context) -> None:
    if isinstance(event, WorkflowStartedEvent):
        event.workflow_inputs = _collect_workflow_inputs(context, event.workflow_id)
    elif isinstance(event, JobStartedEvent):
        now = datetime.now(UTC)
        event.parent_job_id = _get_parent_job_id(context, event.job_id)
        event.dispatch_delay_ms, event.dispatched_at = _get_dispatch_timing(context, event.job_id, now)
        event.job_inputs = _collect_job_inputs(context, event.job_id)


def _collect_workflow_inputs(context: Context, workflow_id: str) -> dict[str, Any]:
    inputs: dict[str, Any] = {}
    try:
        for job_info in context.job_registry.jobs(context):
            if job_info.job.args.parent_id is None:
                job_input = job_info.job.input
                job_type = job_info.job.spec.type
                if isinstance(job_input, dict):
                    for key, value in job_input.items():
                        inputs[f"{job_type}.{key}"] = value
                else:
                    inputs[job_type] = job_input
    except Exception as err:
        log.debug("Could not collect workflow inputs for %s: %s", workflow_id, err)
    return inputs


def _collect_job_inputs(context: Context, job_id: str) -> dict[str, Any] | None:
    try:
        for job_info in context.job_registry.jobs(context):
            if job_info.job_id == job_id:
                job_input = job_info.job.input
                if isinstance(job_input, dict):
                    return dict(job_input)
                if job_input is not None:
                    return {"input": json.dumps(job_input)[:500]}
                return None
    except Exception as err:
        log.debug("Could not collect job inputs for %s: %s", job_id, err)
    return None


def _get_parent_job_id(context: Context, job_id: str) -> str | None:
    try:
        for job_info in context.job_registry.jobs(context):
            if job_info.job_id == job_id:
                return job_info.job.args.parent_id
    except Exception as err:
        log.debug("Could not get parent job id for %s: %s", job_id, err)
    return None


def _get_dispatch_timing(
    context: Context, job_id: str, now: datetime
) -> tuple[float | None, datetime | None]:
    try:
        timing = context.job_registry.get_job_timing(job_id)
        if timing and timing.started_at:
            delay_ms = (now - timing.started_at).total_seconds() * 1000
            return round(delay_ms, 2), timing.started_at
    except Exception as err:
        log.debug("Could not get dispatch timing for %s: %s", job_id, err)
    return None, None
