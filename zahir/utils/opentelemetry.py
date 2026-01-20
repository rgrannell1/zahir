"""OpenTelemetry tracing support for Zahir workflows."""

from collections.abc import Mapping
from datetime import UTC, datetime
import json
import os
import pathlib
import socket
from typing import Any

from zahir.base_types import Context, JobRegistry
from zahir.utils.id_generator import generate_span_id, generate_trace_id
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
    WorkflowCompleteEvent,
    WorkflowStartedEvent,
    ZahirEvent,
)


def _nanoseconds_since_epoch(timestamp: datetime | None) -> int:
    """Convert datetime to nanoseconds since epoch."""
    if timestamp is None:
        return 0
    return int(timestamp.timestamp() * 1_000_000_000)


class FileOtelExporter:
    """Exports OpenTelemetry spans to a file in OTLP JSON format."""

    def __init__(self, file_path: pathlib.Path) -> None:
        self.file_path = file_path
        self.file_handle = file_path.open("a", encoding="utf-8", buffering=1)

    def export_span(self, span_data: dict[str, Any]) -> None:
        # Resource attributes are consistent across all spans (overseer process)
        # Process-specific info is in span attributes (process.pid) for filtering/grouping
        resource_span = {
            "resourceSpans": [
                {
                    "resource": {
                        "attributes": [
                            {"key": "service.name", "value": {"stringValue": "zahir"}},
                            {"key": "service.instance.id", "value": {"stringValue": f"overseer-{os.getpid()}"}},
                            {"key": "host.name", "value": {"stringValue": socket.gethostname()}},
                            {"key": "process.pid", "value": {"intValue": str(os.getpid())}},
                        ],
                    },
                    "scopeSpans": [
                        {
                            "scope": {"name": "zahir"},
                            "spans": [span_data],
                        },
                    ],
                },
            ],
        }
        self.file_handle.write(json.dumps(resource_span) + "\n")
        self.file_handle.flush()

    def close(self) -> None:
        """Close the file handle."""
        if self.file_handle:
            self.file_handle.close()


class TraceContextManager:
    """Manages trace and span context for Zahir workflows.

    This class tracks trace IDs, span IDs, and parent-child relationships
    without modifying event serialization. It creates spans based on events
    and writes them to files in OTLP JSON format.
    """

    def __init__(self, output_dir: str | None, context: Context) -> None:
        """Initialize the trace context manager.

        @param output_dir: Directory to write trace files. If None, tracing is disabled.
        @param context: The workflow context for accessing job registry
        """
        self.output_dir = output_dir
        self.context = context
        self.enabled = output_dir is not None

        self.workflow_traces: dict[str, str] = {}  # workflow_id -> trace_id
        self.job_spans: dict[str, dict[str, Any]] = {}  # job_id -> span data
        self.job_parents: dict[str, str | None] = {}  # job_id -> parent_job_id

        self.exporters: dict[str, FileOtelExporter] = {}

        if self.enabled and output_dir is not None:
            pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)

    def _get_exporter(self, workflow_id: str) -> FileOtelExporter | None:
        """Get or create a file exporter for a workflow.

        @param workflow_id: The workflow ID
        @return: The file exporter, or None if tracing is disabled
        """
        if not self.enabled:
            return None

        if workflow_id not in self.exporters:
            iso_timestamp = datetime.now(UTC).isoformat().replace(":", "-")
            if self.output_dir is None:
                return None
            file_path = pathlib.Path(self.output_dir) / f"traces-{workflow_id}-{iso_timestamp}.jsonl"
            self.exporters[workflow_id] = FileOtelExporter(file_path)

        return self.exporters[workflow_id]

    def _get_parent_span_id(self, job_id: str) -> str | None:
        """Get the parent span ID for a job.

        @param job_id: The job ID
        @return: The parent span ID, or None if no parent
        """
        parent_job_id = self.job_parents.get(job_id)
        if parent_job_id is None:
            return None

        parent_span = self.job_spans.get(parent_job_id)
        if parent_span is None:
            return None

        return parent_span.get("spanId")

    def _get_job_from_registry(self, job_id: str) -> Any | None:
        """Get a job instance from the job registry.

        @param job_id: The job ID
        @return: The JobInstance, or None if not found
        """
        try:
            for job_info in self.context.job_registry.jobs(self.context):
                if job_info.job_id == job_id:
                    return job_info.job
        except Exception:
            pass
        return None

    def _get_job_parent_id(self, job_id: str) -> str | None:
        """Get the parent job ID from the job registry.

        @param job_id: The job ID
        @return: The parent job ID, or None if not found
        """
        # Check if we already have it cached
        if job_id in self.job_parents:
            return self.job_parents[job_id]

        # Try to get it from the job registry
        job = self._get_job_from_registry(job_id)
        if job is not None:
            parent_id: str | None = job.args.parent_id
            self.job_parents[job_id] = parent_id
            return parent_id

        return None

    def _get_workflow_inputs(self, workflow_id: str) -> dict[str, Any]:
        """Get workflow input arguments from root jobs (jobs with no parent).

        @param workflow_id: The workflow ID
        @return: Dictionary of input arguments from root jobs
        """
        inputs: dict[str, Any] = {}
        try:
            for job_info in self.context.job_registry.jobs(self.context):
                # Find root jobs (no parent) that belong to this workflow
                if job_info.job.args.parent_id is None:
                    # Try to match workflow_id - we'll use job_id prefix or check if it's a start job
                    # For now, capture all root jobs' inputs
                    job_input = job_info.job.input
                    if isinstance(job_input, dict):
                        # Merge inputs, prefixing with job type to avoid collisions
                        job_type = job_info.job.spec.type
                        for key, value in job_input.items():
                            inputs[f"{job_type}.{key}"] = value
                    else:
                        # Non-dict input - store as job type
                        inputs[job_info.job.spec.type] = job_input
        except Exception:
            pass
        return inputs

    def _create_span(
        self,
        trace_id: str,
        span_id: str,
        name: str,
        start_time: datetime,
        parent_span_id: str | None = None,
        attributes: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create a span data structure in OTLP format.

        @param trace_id: The trace ID
        @param span_id: The span ID
        @param name: The span name
        @param start_time: The span start time
        @param parent_span_id: Optional parent span ID
        @param attributes: Optional span attributes
        @return: The span data dictionary
        """
        span: dict[str, Any] = {
            "traceId": trace_id,
            "spanId": span_id,
            "name": name,
            "kind": 1,  # SPAN_KIND_INTERNAL
            "startTimeUnixNano": _nanoseconds_since_epoch(start_time),
            "attributes": [],
        }

        if parent_span_id:
            span["parentSpanId"] = parent_span_id

        if attributes:
            for key, value in attributes.items():
                # Convert value to OTLP attribute format
                if isinstance(value, str):
                    span["attributes"].append({"key": key, "value": {"stringValue": value}})
                elif isinstance(value, bool):
                    span["attributes"].append({"key": key, "value": {"boolValue": value}})
                elif isinstance(value, int):
                    span["attributes"].append({"key": key, "value": {"intValue": str(value)}})
                elif isinstance(value, float):
                    span["attributes"].append({"key": key, "value": {"doubleValue": value}})
                elif value is None:
                    continue
                else:
                    # Convert other types to string
                    span["attributes"].append({"key": key, "value": {"stringValue": str(value)}})

        return span

    def _end_span(
        self, job_id: str, end_time: datetime, status_code: int = 1, status_message: str | None = None
    ) -> None:
        """End a span and export it.

        @param job_id: The job ID
        @param end_time: The span end time
        @param status_code: 1 for OK, 2 for ERROR
        @param status_message: Optional status message
        """
        span = self.job_spans.get(job_id)
        if span is None:
            return

        span["endTimeUnixNano"] = _nanoseconds_since_epoch(end_time)
        span["status"] = {"code": status_code}
        if status_message:
            span["status"]["message"] = status_message

        # Find the workflow_id for this job
        workflow_id = None
        for wf_id, trace_id in self.workflow_traces.items():
            if trace_id == span["traceId"]:
                workflow_id = wf_id
                break

        if workflow_id:
            exporter = self._get_exporter(workflow_id)
            if exporter:
                exporter.export_span(span)

        # Remove from active spans
        del self.job_spans[job_id]

    def handle_event(self, event: ZahirEvent) -> None:
        """Handle a workflow event and update trace context.

        @param event: The event to process
        """
        if not self.enabled:
            return

        if isinstance(event, WorkflowStartedEvent):
            _handle_workflow_started(self, event)
        elif isinstance(event, WorkflowCompleteEvent):
            _handle_workflow_complete(self, event)
        elif isinstance(event, JobStartedEvent):
            _handle_job_started(self, event)
        elif isinstance(event, JobCompletedEvent):
            _handle_job_completed(self, event)
        elif isinstance(event, JobTimeoutEvent):
            _handle_job_timeout(self, event)
        elif isinstance(event, JobIrrecoverableEvent):
            _handle_job_irrecoverable(self, event)
        elif isinstance(event, JobPrecheckFailedEvent):
            _handle_job_precheck_failed(self, event)
        elif isinstance(event, JobPausedEvent):
            _handle_job_paused(self, event)
        elif isinstance(event, JobImpossibleEvent):
            _handle_job_impossible(self, event)
        elif isinstance(event, JobRecoveryStartedEvent):
            _handle_job_recovery_started(self, event)
        elif isinstance(event, JobRecoveryCompletedEvent):
            _handle_job_recovery_completed(self, event)
        elif isinstance(event, JobRecoveryTimeoutEvent):
            _handle_job_recovery_timeout(self, event)
        elif isinstance(event, JobOutputEvent):
            _handle_job_output(self, event)

    def close(self) -> None:
        """Close all file exporters and clean up."""
        for exporter in self.exporters.values():
            exporter.close()
        self.exporters.clear()


def _handle_workflow_started(manager: TraceContextManager, event: WorkflowStartedEvent) -> None:
    """Handle WorkflowStartedEvent."""
    trace_id = generate_trace_id()
    manager.workflow_traces[event.workflow_id] = trace_id

    # Get workflow inputs from root jobs in the registry
    workflow_inputs = manager._get_workflow_inputs(event.workflow_id)

    # Create root span for the workflow
    span_id = generate_span_id()
    start_time = datetime.now(UTC)
    attributes: dict[str, Any] = {
        "workflow.id": event.workflow_id,
        "workflow.pid": event.pid,
        "process.pid": event.pid,
    }

    # Add workflow inputs as attributes (truncate if needed)
    for key, value in workflow_inputs.items():
        if isinstance(value, (str, int, float, bool)):
            attributes[f"workflow.input.{key}"] = value
        else:
            # Serialize complex types and truncate
            input_str = json.dumps(value)[:500]
            attributes[f"workflow.input.{key}"] = input_str

    span = manager._create_span(
        trace_id=trace_id,
        span_id=span_id,
        name=f"workflow:{event.workflow_id}",
        start_time=start_time,
        attributes=attributes,
    )
    # Store workflow span with a special key
    manager.job_spans[f"_workflow:{event.workflow_id}"] = span


def _handle_workflow_complete(manager: TraceContextManager, event: WorkflowCompleteEvent) -> None:
    """Handle WorkflowCompleteEvent."""
    workflow_key = f"_workflow:{event.workflow_id}"
    if workflow_key in manager.job_spans:
        span = manager.job_spans[workflow_key]
        span["endTimeUnixNano"] = _nanoseconds_since_epoch(datetime.now(UTC))
        span["status"] = {"code": 1}  # OK
        span["attributes"].append({
            "key": "workflow.duration_seconds",
            "value": {"doubleValue": event.duration_seconds},
        })

        exporter = manager._get_exporter(event.workflow_id)
        if exporter:
            exporter.export_span(span)
        del manager.job_spans[workflow_key]


def _handle_job_started(manager: TraceContextManager, event: JobStartedEvent) -> None:
    """Handle JobStartedEvent."""
    trace_id = manager.workflow_traces.get(event.workflow_id)
    if trace_id is None:
        return

    # Get parent span ID
    parent_span_id = manager._get_parent_span_id(event.job_id)
    if parent_span_id is None:
        # Try to get parent from job registry
        parent_job_id = manager._get_job_parent_id(event.job_id)
        if parent_job_id:
            parent_span_id = manager._get_parent_span_id(parent_job_id)

    # Get job inputs from the registry
    job = manager._get_job_from_registry(event.job_id)
    attributes: dict[str, Any] = {
        "job.id": event.job_id,
        "job.type": event.job_type,
        "job.workflow_id": event.workflow_id,
        "job.worker_pid": event.pid,
        "process.pid": event.pid,
    }

    # Add job inputs as attributes
    if job is not None:
        job_input = job.input
        if isinstance(job_input, dict):
            for key, value in job_input.items():
                if isinstance(value, (str, int, float, bool)):
                    attributes[f"job.input.{key}"] = value
                else:
                    # Serialize complex types and truncate
                    input_str = json.dumps(value)[:500]
                    attributes[f"job.input.{key}"] = input_str
        elif job_input is not None:
            # Non-dict input - serialize and truncate
            input_str = json.dumps(job_input)[:500]
            attributes["job.input"] = input_str

    span_id = generate_span_id()
    start_time = datetime.now(UTC)
    span = manager._create_span(
        trace_id=trace_id,
        span_id=span_id,
        name=f"job:{event.job_type}",
        start_time=start_time,
        parent_span_id=parent_span_id,
        attributes=attributes,
    )
    manager.job_spans[event.job_id] = span


def _handle_job_completed(manager: TraceContextManager, event: JobCompletedEvent) -> None:
    """Handle JobCompletedEvent."""
    manager._end_span(
        job_id=event.job_id,
        end_time=datetime.now(UTC),
        status_code=1,  # OK
    )


def _handle_job_timeout(manager: TraceContextManager, event: JobTimeoutEvent) -> None:
    """Handle JobTimeoutEvent."""
    span = manager.job_spans.get(event.job_id)
    if span is not None:
        span["attributes"].append({"key": "job.timeout_seconds", "value": {"doubleValue": event.duration_seconds}})
    manager._end_span(
        job_id=event.job_id,
        end_time=datetime.now(UTC),
        status_code=2,  # ERROR
        status_message=f"Job timed out after {event.duration_seconds}s",
    )


def _handle_job_irrecoverable(manager: TraceContextManager, event: JobIrrecoverableEvent) -> None:
    """Handle JobIrrecoverableEvent."""
    span = manager.job_spans.get(event.job_id)
    if span is not None:
        error_msg = str(event.error)
        span["attributes"].append({"key": "job.error", "value": {"stringValue": error_msg}})
        span["attributes"].append({"key": "job.error_type", "value": {"stringValue": type(event.error).__name__}})
    manager._end_span(
        job_id=event.job_id,
        end_time=datetime.now(UTC),
        status_code=2,  # ERROR
        status_message=f"Job failed irrecoverably: {str(event.error)}",
    )


def _handle_job_precheck_failed(manager: TraceContextManager, event: JobPrecheckFailedEvent) -> None:
    """Handle JobPrecheckFailedEvent."""
    span = manager.job_spans.get(event.job_id)
    if span is not None:
        span["attributes"].append({"key": "job.precheck_error", "value": {"stringValue": event.error}})
    manager._end_span(
        job_id=event.job_id,
        end_time=datetime.now(UTC),
        status_code=2,  # ERROR
        status_message=f"Job precheck failed: {event.error}",
    )


def _handle_job_paused(manager: TraceContextManager, event: JobPausedEvent) -> None:
    """Handle JobPausedEvent."""
    # Paused jobs are still active, just update attributes
    span = manager.job_spans.get(event.job_id)
    if span is not None:
        span["attributes"].append({"key": "job.paused", "value": {"boolValue": True}})


def _handle_job_impossible(manager: TraceContextManager, event: JobImpossibleEvent) -> None:
    """Handle JobImpossibleEvent."""
    span = manager.job_spans.get(event.job_id)
    if span is not None:
        span["attributes"].append({"key": "job.impossible", "value": {"boolValue": True}})
    manager._end_span(
        job_id=event.job_id,
        end_time=datetime.now(UTC),
        status_code=2,  # ERROR
        status_message="Job marked as impossible",
    )


def _handle_job_recovery_started(manager: TraceContextManager, event: JobRecoveryStartedEvent) -> None:
    """Handle JobRecoveryStartedEvent."""
    # Create a recovery span as a child of the job span
    trace_id = manager.workflow_traces.get(event.workflow_id)
    if trace_id is None:
        return

    parent_span_id: str | None = manager._get_parent_span_id(event.job_id)
    if parent_span_id is None:
        # Use the job's own span as parent
        job_span = manager.job_spans.get(event.job_id)
        if job_span is not None:
            parent_span_id = job_span.get("spanId")

    if parent_span_id:
        span_id = generate_span_id()
        start_time = datetime.now(UTC)
        span = manager._create_span(
            trace_id=trace_id,
            span_id=span_id,
            name=f"job.recovery:{event.job_type}",
            start_time=start_time,
            parent_span_id=parent_span_id,
            attributes={
                "job.id": event.job_id,
                "job.type": event.job_type,
                "job.recovery": True,
            },
        )
        manager.job_spans[f"{event.job_id}:recovery"] = span


def _handle_job_recovery_completed(manager: TraceContextManager, event: JobRecoveryCompletedEvent) -> None:
    """Handle JobRecoveryCompletedEvent."""
    recovery_key = f"{event.job_id}:recovery"
    if recovery_key in manager.job_spans:
        manager._end_span(
            job_id=recovery_key,
            end_time=datetime.now(UTC),
            status_code=1,  # OK
        )


def _handle_job_recovery_timeout(manager: TraceContextManager, event: JobRecoveryTimeoutEvent) -> None:
    """Handle JobRecoveryTimeoutEvent."""
    recovery_key = f"{event.job_id}:recovery"
    if recovery_key in manager.job_spans:
        span = manager.job_spans[recovery_key]
        span["attributes"].append({
            "key": "job.recovery.timeout_seconds",
            "value": {"doubleValue": event.duration_seconds},
        })
        manager._end_span(
            job_id=recovery_key,
            end_time=datetime.now(UTC),
            status_code=2,  # ERROR
            status_message="Recovery timed out",
        )


def _handle_job_output(manager: TraceContextManager, event: JobOutputEvent) -> None:
    """Handle JobOutputEvent."""
    # Add output as an attribute to the job span
    job_id = event.job_id or ""
    span = manager.job_spans.get(job_id)
    if span is not None and event.output:
        # Truncate pls
        output_summary = json.dumps(event.output)[:500]  # Limit to 500 chars
        span["attributes"].append({"key": "job.output_summary", "value": {"stringValue": output_summary}})
