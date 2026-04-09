"""Framework-independent monitoring for Zahir workflows.

Lifecycle events (workflow started/completed, job started/failed/etc.) are
ordinary ``ZahirEvent`` subclasses from ``zahir.events``, enriched with extra
monitoring fields before being passed to the emitter:

- ``WorkflowStartedEvent.workflow_inputs`` — inputs of root jobs
- ``JobStartedEvent.parent_job_id`` — parent job if any
- ``JobStartedEvent.dispatch_delay_ms`` / ``dispatched_at`` — scheduling lag
- ``JobStartedEvent.job_inputs`` — job input values

Performance measurements use dedicated event types defined here.

Usage::

    from zahir.monitoring import MonitoringEmitter, MeasurementEvent
    from zahir.events import ZahirEvent


    class MyEmitter:
        def emit(self, event: ZahirEvent | MeasurementEvent) -> None:
            print(event)


    workflow = LocalWorkflow(monitoring_emitter=MyEmitter())
"""

from zahir.monitoring.emitter import MonitoringEmitter, NullEmitter
from zahir.monitoring.events import (
    DepWorkerLoopStats,
    MeasurementEvent,
    OverseerDispatchStats,
    OverseerThroughputStats,
)

__all__ = [
    "MonitoringEmitter",
    "MonitoringEvent",
    "NullEmitter",
    "DepWorkerLoopStats",
    "MeasurementEvent",
    "OverseerDispatchStats",
    "OverseerThroughputStats",
]
