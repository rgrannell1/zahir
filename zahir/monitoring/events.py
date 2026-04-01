"""Performance measurement events for Zahir.

These are emitted periodically by internal workers and carry raw values so
any backend can build its own histograms and time-series.

Workflow/job lifecycle events are carried by the enriched ZahirEvent types
in ``zahir.events`` (e.g. ``JobStartedEvent.parent_job_id``).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


class MeasurementEvent:
    """Base for periodic performance snapshots emitted by internal workers."""


@dataclass
class DepWorkerLoopStats(MeasurementEvent):
    """Measurements for one iteration of the dependency-checker loop."""

    timestamp: datetime
    loop_duration_ms: float
    pending_jobs_checked: int
    jobs_made_ready: int
    active_workflows: int
    get_state_calls: int


@dataclass
class OverseerDispatchStats(MeasurementEvent):
    """Measurements for one call to dispatch_jobs_to_workers."""

    timestamp: datetime
    dispatch_duration_ms: float
    jobs_dispatched: int


@dataclass
class OverseerThroughputStats(MeasurementEvent):
    """Throughput snapshot recorded after each job completion."""

    timestamp: datetime
    jobs_per_second: float
    jobs_completed_total: int
    completion_interval_ms: float | None = None
