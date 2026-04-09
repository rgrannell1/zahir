"""Tests for zahir.monitoring — emitter protocol, measurement events, and enrichment."""

from __future__ import annotations

from datetime import UTC, datetime
import io
import json

import pytest

from zahir.events import (
    JobCompletedEvent,
    JobImpossibleEvent,
    JobIrrecoverableEvent,
    JobPausedEvent,
    JobPrecheckFailedEvent,
    JobStartedEvent,
    JobTimeoutEvent,
    WorkflowStartedEvent,
)
from zahir.monitoring import (
    DepWorkerLoopStats,
    MeasurementEvent,
    MonitoringEmitter,
    NullEmitter,
    OverseerDispatchStats,
    OverseerThroughputStats,
)
from zahir.monitoring.events import MeasurementEvent


# ── NullEmitter ───────────────────────────────────────────────────────────────


class TestNullEmitter:
    def test_emit_zahir_event(self):
        emitter = NullEmitter()
        event = WorkflowStartedEvent(workflow_id="wf-1", pid=1)
        emitter.emit(event)  # must not raise

    def test_emit_measurement_event(self):
        emitter = NullEmitter()
        event = DepWorkerLoopStats(
            timestamp=datetime.now(UTC),
            loop_duration_ms=5.0,
            pending_jobs_checked=2,
            jobs_made_ready=1,
            active_workflows=1,
            get_state_calls=2,
        )
        emitter.emit(event)  # must not raise

    def test_satisfies_protocol(self):
        assert isinstance(NullEmitter(), MonitoringEmitter)


# ── Protocol conformance ──────────────────────────────────────────────────────


class TestMonitoringEmitterProtocol:
    def test_custom_class_satisfies_protocol(self):
        class MyEmitter:
            def emit(self, event) -> None:
                pass

        assert isinstance(MyEmitter(), MonitoringEmitter)

    def test_class_without_emit_fails(self):
        class NotAnEmitter:
            pass

        assert not isinstance(NotAnEmitter(), MonitoringEmitter)


# ── Enrichment fields on ZahirEvents ─────────────────────────────────────────


class TestWorkflowStartedEnrichment:
    def test_defaults_to_empty_inputs(self):
        event = WorkflowStartedEvent(workflow_id="wf-1", pid=1)
        assert event.workflow_inputs == {}

    def test_enrichment_field_excluded_from_equality(self):
        a = WorkflowStartedEvent(workflow_id="wf-1", pid=1)
        b = WorkflowStartedEvent(workflow_id="wf-1", pid=1)
        b.workflow_inputs = {"job.x": 1}
        assert a == b  # compare=False keeps equality stable

    def test_can_set_workflow_inputs(self):
        event = WorkflowStartedEvent(workflow_id="wf-1", pid=1)
        event.workflow_inputs = {"job.key": "value"}
        assert event.workflow_inputs == {"job.key": "value"}


class TestJobStartedEnrichment:
    def _make_event(self) -> JobStartedEvent:
        return JobStartedEvent(workflow_id="wf-1", job_id="j-1", job_type="my_job", pid=1)

    def test_all_enrichment_fields_default_to_none(self):
        event = self._make_event()
        assert event.parent_job_id is None
        assert event.dispatch_delay_ms is None
        assert event.dispatched_at is None
        assert event.job_inputs is None

    def test_enrichment_fields_excluded_from_equality(self):
        a = self._make_event()
        b = self._make_event()
        b.parent_job_id = "parent-99"
        b.dispatch_delay_ms = 42.0
        b.job_inputs = {"x": 1}
        assert a == b  # compare=False

    def test_can_set_all_enrichment_fields(self):
        event = self._make_event()
        ts = datetime.now(UTC)
        event.parent_job_id = "parent-1"
        event.dispatch_delay_ms = 12.5
        event.dispatched_at = ts
        event.job_inputs = {"key": "val"}

        assert event.parent_job_id == "parent-1"
        assert event.dispatch_delay_ms == 12.5
        assert event.dispatched_at == ts
        assert event.job_inputs == {"key": "val"}

    def test_save_does_not_include_enrichment(self):
        """Enrichment fields are not serialized (they are set after deserialization)."""
        from zahir.context.memory import MemoryContext
        from zahir.job_registry.sqlite import SQLiteJobRegistry
        from zahir.scope import LocalScope

        scope = LocalScope()
        registry = SQLiteJobRegistry(":memory:")
        context = MemoryContext(scope=scope, job_registry=registry)

        event = self._make_event()
        event.parent_job_id = "parent-1"
        event.dispatch_delay_ms = 10.0
        saved = event.save(context)

        assert "parent_job_id" not in saved
        assert "dispatch_delay_ms" not in saved


# ── Measurement events ────────────────────────────────────────────────────────


class TestDepWorkerLoopStats:
    def test_fields(self):
        ts = datetime.now(UTC)
        event = DepWorkerLoopStats(
            timestamp=ts,
            loop_duration_ms=12.3,
            pending_jobs_checked=5,
            jobs_made_ready=2,
            active_workflows=3,
            get_state_calls=10,
        )
        assert event.loop_duration_ms == 12.3
        assert event.jobs_made_ready == 2
        assert isinstance(event, MeasurementEvent)


class TestOverseerDispatchStats:
    def test_fields(self):
        ts = datetime.now(UTC)
        event = OverseerDispatchStats(timestamp=ts, dispatch_duration_ms=0.5, jobs_dispatched=3)
        assert event.jobs_dispatched == 3
        assert isinstance(event, MeasurementEvent)


class TestOverseerThroughputStats:
    def test_fields(self):
        ts = datetime.now(UTC)
        event = OverseerThroughputStats(
            timestamp=ts,
            jobs_per_second=10.0,
            jobs_completed_total=100,
            completion_interval_ms=50.0,
        )
        assert event.jobs_per_second == 10.0
        assert event.completion_interval_ms == 50.0

    def test_interval_optional(self):
        event = OverseerThroughputStats(timestamp=datetime.now(UTC), jobs_per_second=1.0, jobs_completed_total=1)
        assert event.completion_interval_ms is None



# ── Emitter collects all expected event types ─────────────────────────────────


class TestEmitterReceivesExpectedEvents:
    def test_zahir_lifecycle_events_reach_emitter(self):
        received = []

        class Collector:
            def emit(self, event) -> None:
                received.append(event)

        emitter = Collector()
        ts = datetime.now(UTC)

        lifecycle_events = [
            WorkflowStartedEvent(workflow_id="w", pid=1),
            JobStartedEvent(workflow_id="w", job_id="j", job_type="t", pid=1),
            JobCompletedEvent(workflow_id="w", job_id="j", job_type="t", pid=1, duration_seconds=1.0),
            JobTimeoutEvent(workflow_id="w", job_id="j", job_type="t", pid=1, duration_seconds=5.0),
            JobIrrecoverableEvent(workflow_id="w", job_id="j", job_type="t", pid=1, error="oops"),
            JobPrecheckFailedEvent(workflow_id="w", job_id="j", job_type="t", pid=1, error="bad"),
            JobImpossibleEvent(workflow_id="w", job_id="j", job_type="t", pid=1),
            JobPausedEvent(workflow_id="w", job_id="j", job_type="t", pid=1),
        ]
        measurement_events = [
            DepWorkerLoopStats(
                timestamp=ts,
                loop_duration_ms=1.0,
                pending_jobs_checked=0,
                jobs_made_ready=0,
                active_workflows=1,
                get_state_calls=0,
            ),
            OverseerDispatchStats(timestamp=ts, dispatch_duration_ms=0.1, jobs_dispatched=1),
            OverseerThroughputStats(timestamp=ts, jobs_per_second=1.0, jobs_completed_total=1),
        ]

        for event in lifecycle_events + measurement_events:
            emitter.emit(event)

        assert len(received) == len(lifecycle_events) + len(measurement_events)
