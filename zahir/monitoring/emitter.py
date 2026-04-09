"""MonitoringEmitter protocol and built-in implementations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from zahir.events import ZahirEvent
    from zahir.monitoring.events import MeasurementEvent

    EmittableEvent = "ZahirEvent | MeasurementEvent"


@runtime_checkable
class MonitoringEmitter(Protocol):
    """Receive monitoring events from the Zahir runtime.

    The ``emit`` method is called with either:
    - A ``ZahirEvent`` subclass (workflow/job lifecycle).  Job-start events are
      enriched with ``parent_job_id``, ``dispatch_delay_ms``, ``dispatched_at``,
      and ``job_inputs`` before being passed here; workflow-start events carry
      ``workflow_inputs``.
    - A ``MeasurementEvent`` subclass (``DepWorkerLoopStats``,
      ``OverseerDispatchStats``, ``OverseerThroughputStats``).

    ``emit`` is called synchronously in the overseer process and should not
    block for long.
    """

    def emit(self, event: ZahirEvent | MeasurementEvent) -> None: ...


class NullEmitter:
    """A no-op emitter; all events are silently discarded."""

    def emit(self, event: ZahirEvent | MeasurementEvent) -> None:
        pass
