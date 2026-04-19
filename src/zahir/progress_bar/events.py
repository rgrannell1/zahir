from dataclasses import dataclass, field


@dataclass
class ZahirEvent:
    """Base class for all zahir internal events."""


@dataclass
class ZahirTelemetryEvent(ZahirEvent):
    """Telemetry event emitted when a job effect begins."""

    span_id: str
    tag: str
    event: str
    timestamp: float
    attributes: dict = field(default_factory=dict)


@dataclass
class ZahirSpanEnd(ZahirTelemetryEvent):
    """Telemetry event emitted when a job effect completes."""

    duration_ms: float = 0.0
    error: str | None = None
