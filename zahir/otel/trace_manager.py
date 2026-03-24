"""Top-level trace-context manager that wires events to span lifecycle."""

from __future__ import annotations

from datetime import UTC, datetime
import logging
import pathlib
from typing import Any

from zahir.base_types import Context
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
    ZahirEvent,
)
from zahir.otel.event_handlers import (
    handle_job_completed,
    handle_job_impossible,
    handle_job_irrecoverable,
    handle_job_output,
    handle_job_paused,
    handle_job_precheck_failed,
    handle_job_recovery_completed,
    handle_job_recovery_started,
    handle_job_recovery_timeout,
    handle_job_started,
    handle_job_timeout,
    handle_job_worker_waiting,
    handle_workflow_complete,
    handle_workflow_started,
)
from zahir.otel.exporter import FileSpanExporter
from zahir.otel.spans import finalise_span, new_span
from zahir.utils.id_generator import generate_trace_id

log = logging.getLogger(__name__)

# Map event types to handler functions.  Order doesn't matter; this replaces
# the long isinstance chain.
EVENT_HANDLERS: dict[type[ZahirEvent], Any] = {
    WorkflowStartedEvent: handle_workflow_started,
    WorkflowCompleteEvent: handle_workflow_complete,
    JobWorkerWaitingEvent: handle_job_worker_waiting,
    JobStartedEvent: handle_job_started,
    JobCompletedEvent: handle_job_completed,
    JobTimeoutEvent: handle_job_timeout,
    JobIrrecoverableEvent: handle_job_irrecoverable,
    JobPrecheckFailedEvent: handle_job_precheck_failed,
    JobPausedEvent: handle_job_paused,
    JobImpossibleEvent: handle_job_impossible,
    JobRecoveryStartedEvent: handle_job_recovery_started,
    JobRecoveryCompletedEvent: handle_job_recovery_completed,
    JobRecoveryTimeoutEvent: handle_job_recovery_timeout,
    JobOutputEvent: handle_job_output,
}


class TraceContextManager:
    """Manages trace/span state and delegates events to handler functions.

    Responsibilities
    ----------------
    * Maintains the mapping from workflow IDs to trace IDs.
    * Stores live (in-flight) spans keyed by a string identifier.
    * Owns one ``FileSpanExporter`` per workflow for writing OTLP JSON.
    * Routes incoming ``ZahirEvent`` instances to the correct handler.
    """

    def __init__(self, output_dir: str | None, context: Context) -> None:
        self.output_dir = output_dir
        self.context = context
        self.enabled = output_dir is not None

        # workflow_id → trace_id
        self.workflow_traces: dict[str, str] = {}
        # span key → live span dict
        self.spans: dict[str, dict[str, Any]] = {}
        # job_id → parent job_id (cached)
        self.job_parents: dict[str, str | None] = {}
        # pid → live process span dict
        self.process_spans: dict[int, dict[str, Any]] = {}
        # pid → workflow_id
        self.process_workflows: dict[int, str] = {}
        # workflow_id → FileSpanExporter
        self.exporters: dict[str, FileSpanExporter] = {}

        if self.enabled and output_dir is not None:
            pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)

    # ── Public API used by event handlers ──────────────────────────

    def handle_event(self, event: ZahirEvent) -> None:
        """Route *event* to the appropriate handler."""
        if not self.enabled:
            return

        handler = EVENT_HANDLERS.get(type(event))
        if handler is not None:
            handler(self, event)

    # ── Workflow registration ──────────────────────────────────────

    def register_workflow(self, workflow_id: str) -> str:
        """Create and store a new trace ID for *workflow_id*."""
        trace_id = generate_trace_id()
        self.workflow_traces[workflow_id] = trace_id
        return trace_id

    def trace_id_for(self, workflow_id: str) -> str | None:
        """Return the trace ID for *workflow_id*, or None."""
        return self.workflow_traces.get(workflow_id)

    # ── Span storage ──────────────────────────────────────────────

    def store_span(self, key: str, span: dict[str, Any]) -> None:
        """Store a live span under *key*."""
        self.spans[key] = span

    def get_span(self, key: str) -> dict[str, Any] | None:
        """Retrieve a live span by *key*."""
        return self.spans.get(key)

    def remove_span(self, key: str) -> None:
        """Remove a span from the live set (does not export)."""
        self.spans.pop(key, None)

    def end_span(
        self,
        key: str,
        end_time: datetime,
        status_code: int = 1,
        status_message: str | None = None,
    ) -> None:
        """Finalise, export, and remove a span in one step."""
        span = self.spans.get(key)
        if span is None:
            return

        finalise_span(span, end_time, status_code, status_message)

        workflow_id = self.workflow_id_for_trace(span["traceId"])
        if workflow_id:
            self.export_span(workflow_id, span)

        del self.spans[key]

    # ── Export ─────────────────────────────────────────────────────

    def export_span(self, workflow_id: str, span: dict[str, Any]) -> None:
        """Write *span* to the file exporter for *workflow_id*."""
        exporter = self.get_exporter(workflow_id)
        if exporter:
            exporter.export(span)

    def get_exporter(self, workflow_id: str) -> FileSpanExporter | None:
        """Lazily create and return the exporter for *workflow_id*."""
        if not self.enabled or self.output_dir is None:
            return None

        if workflow_id not in self.exporters:
            iso_timestamp = datetime.now(UTC).isoformat().replace(":", "-")
            file_path = pathlib.Path(self.output_dir) / f"traces-{workflow_id}-{iso_timestamp}.jsonl"
            self.exporters[workflow_id] = FileSpanExporter(file_path)

        return self.exporters[workflow_id]

    # ── Process spans ─────────────────────────────────────────────

    def associate_process(self, pid: int, workflow_id: str) -> None:
        """Record that *pid* belongs to *workflow_id*."""
        if pid not in self.process_workflows:
            self.process_workflows[pid] = workflow_id

    def workflow_for_process(self, pid: int) -> str | None:
        """Return the workflow ID associated with *pid*."""
        return self.process_workflows.get(pid)

    def ensure_process_span(self, pid: int, workflow_id: str, start_time: datetime) -> str | None:
        """Return (creating if necessary) the span ID for the process span of *pid*.

        The process span is a child of the workflow root span.
        """
        if pid in self.process_spans:
            return self.process_spans[pid]["spanId"]

        trace_id = self.workflow_traces.get(workflow_id)
        if trace_id is None:
            return None

        workflow_span = self.spans.get(f"_workflow:{workflow_id}")
        parent_span_id = workflow_span["spanId"] if workflow_span else None

        span = new_span(
            trace_id=trace_id,
            name=f"process:worker-{pid}",
            start_time=start_time,
            parent_span_id=parent_span_id,
            attributes={
                "process.pid": pid,
                "process.type": "worker",
            },
        )
        self.process_spans[pid] = span
        self.process_workflows[pid] = workflow_id
        return span["spanId"]

    def end_all_process_spans(self, workflow_id: str, end_time: datetime) -> None:
        """Finalise and export every process span belonging to *workflow_id*."""
        pids = [pid for pid, wf in self.process_workflows.items() if wf == workflow_id]
        for pid in pids:
            span = self.process_spans.pop(pid, None)
            if span is None:
                continue

            finalise_span(span, end_time)
            self.export_span(workflow_id, span)
            self.process_workflows.pop(pid, None)

    # ── Job registry helpers ──────────────────────────────────────

    def job_from_registry(self, job_id: str) -> Any | None:
        """Look up a job instance by *job_id* in the context's registry."""
        try:
            for job_info in self.context.job_registry.jobs(self.context):
                if job_info.job_id == job_id:
                    return job_info.job
        except Exception as err:
            log.debug("Could not look up job %s: %s", job_id, err)
        return None

    def parent_job_id(self, job_id: str) -> str | None:
        """Return the parent job ID for *job_id* (cached)."""
        if job_id in self.job_parents:
            return self.job_parents[job_id]

        job = self.job_from_registry(job_id)
        if job is not None:
            parent_id: str | None = job.args.parent_id
            self.job_parents[job_id] = parent_id
            return parent_id
        return None

    def collect_workflow_inputs(self, workflow_id: str) -> dict[str, Any]:
        """Gather input attributes from root jobs (those with no parent)."""
        inputs: dict[str, Any] = {}
        try:
            for job_info in self.context.job_registry.jobs(self.context):
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

    # ── Reverse lookup ────────────────────────────────────────────

    def workflow_id_for_trace(self, trace_id: str) -> str | None:
        """Find the workflow ID that owns *trace_id*."""
        for wf_id, tid in self.workflow_traces.items():
            if tid == trace_id:
                return wf_id
        return None

    # ── Teardown ──────────────────────────────────────────────────

    def close(self) -> None:
        """Close all file exporters and release resources."""
        for exporter in self.exporters.values():
            exporter.close()
        self.exporters.clear()
