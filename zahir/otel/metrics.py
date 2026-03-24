"""Lightweight in-process metrics that serialise to OTLP JSON.

This keeps parity with the hand-rolled tracing in ``zahir.otel`` - no
SDK dependency required.  Three instrument types are supported:

* **Counter** - monotonically increasing sum (e.g. ``jobs_completed``).
* **Gauge** - point-in-time value (e.g. ``ready_job_count``).
* **Histogram** - distribution summary emitted as explicit-bucket
  histogram (e.g. ``dependency_loop_duration_ms``).

Typical usage::

    metrics = MetricsCollector()
    metrics.counter("jobs_completed", 1, {"workflow_id": "abc"})
    metrics.histogram("dep_loop_ms", 42.3)
    metrics.flush(exporter)          # writes one JSONL line
"""

from __future__ import annotations

import math
import time
from typing import Any

# Default histogram bucket boundaries (milliseconds).
DEFAULT_HISTOGRAM_BOUNDS: list[float] = [
    1, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 30_000, 60_000,
]


def _epoch_nanos() -> int:
    return int(time.time() * 1_000_000_000)


# ── Instruments ──────────────────────────────────────────────────


class _Counter:
    """Monotonic sum."""

    def __init__(self, name: str, description: str, unit: str) -> None:
        self.name = name
        self.description = description
        self.unit = unit
        self.value: float = 0
        self.start_time_nanos = _epoch_nanos()

    def add(self, amount: float = 1) -> None:
        self.value += amount

    def to_otlp(self) -> dict[str, Any]:
        now = _epoch_nanos()
        return {
            "name": self.name,
            "description": self.description,
            "unit": self.unit,
            "sum": {
                "dataPoints": [
                    {
                        "startTimeUnixNano": str(self.start_time_nanos),
                        "timeUnixNano": str(now),
                        "asDouble": self.value,
                        "isMonotonic": True,
                        "aggregationTemporality": 2,  # CUMULATIVE
                    },
                ],
            },
        }


class _Gauge:
    """Last-value gauge."""

    def __init__(self, name: str, description: str, unit: str) -> None:
        self.name = name
        self.description = description
        self.unit = unit
        self.value: float = 0

    def set(self, value: float) -> None:
        self.value = value

    def to_otlp(self) -> dict[str, Any]:
        now = _epoch_nanos()
        return {
            "name": self.name,
            "description": self.description,
            "unit": self.unit,
            "gauge": {
                "dataPoints": [
                    {
                        "timeUnixNano": str(now),
                        "asDouble": self.value,
                    },
                ],
            },
        }


class _Histogram:
    """Explicit-bucket histogram."""

    def __init__(
        self,
        name: str,
        description: str,
        unit: str,
        bounds: list[float] | None = None,
    ) -> None:
        self.name = name
        self.description = description
        self.unit = unit
        self.bounds = bounds or DEFAULT_HISTOGRAM_BOUNDS
        self.bucket_counts = [0] * (len(self.bounds) + 1)
        self.count = 0
        self.total = 0.0
        self.minimum = math.inf
        self.maximum = -math.inf
        self.start_time_nanos = _epoch_nanos()

    def record(self, value: float) -> None:
        self.count += 1
        self.total += value
        self.minimum = min(self.minimum, value)
        self.maximum = max(self.maximum, value)

        for idx, bound in enumerate(self.bounds):
            if value <= bound:
                self.bucket_counts[idx] += 1
                return
        self.bucket_counts[-1] += 1

    def to_otlp(self) -> dict[str, Any]:
        now = _epoch_nanos()
        return {
            "name": self.name,
            "description": self.description,
            "unit": self.unit,
            "histogram": {
                "dataPoints": [
                    {
                        "startTimeUnixNano": str(self.start_time_nanos),
                        "timeUnixNano": str(now),
                        "count": str(self.count),
                        "sum": self.total,
                        "min": self.minimum if self.count > 0 else 0,
                        "max": self.maximum if self.count > 0 else 0,
                        "bucketCounts": [str(count) for count in self.bucket_counts],
                        "explicitBounds": self.bounds,
                        "aggregationTemporality": 2,  # CUMULATIVE
                    },
                ],
            },
        }


# ── Collector ────────────────────────────────────────────────────


class MetricsCollector:
    """Registry for all metric instruments.

    Call :meth:`flush` periodically to write the current snapshot to a
    ``FileMetricsExporter``.
    """

    def __init__(self) -> None:
        self._counters: dict[str, _Counter] = {}
        self._gauges: dict[str, _Gauge] = {}
        self._histograms: dict[str, _Histogram] = {}

    # ── instrument creation / lookup ──

    def get_counter(self, name: str, description: str = "", unit: str = "") -> _Counter:
        if name not in self._counters:
            self._counters[name] = _Counter(name, description, unit)
        return self._counters[name]

    def get_gauge(self, name: str, description: str = "", unit: str = "") -> _Gauge:
        if name not in self._gauges:
            self._gauges[name] = _Gauge(name, description, unit)
        return self._gauges[name]

    def get_histogram(
        self,
        name: str,
        description: str = "",
        unit: str = "",
        bounds: list[float] | None = None,
    ) -> _Histogram:
        if name not in self._histograms:
            self._histograms[name] = _Histogram(name, description, unit, bounds)
        return self._histograms[name]

    # ── convenience shortcuts ──

    def counter(self, name: str, amount: float = 1, description: str = "", unit: str = "") -> None:
        self.get_counter(name, description, unit).add(amount)

    def gauge(self, name: str, value: float, description: str = "", unit: str = "") -> None:
        self.get_gauge(name, description, unit).set(value)

    def histogram(self, name: str, value: float, description: str = "", unit: str = "") -> None:
        self.get_histogram(name, description, unit).record(value)

    # ── export ──

    def snapshot(self) -> list[dict[str, Any]]:
        """Return OTLP metric dicts for every instrument."""
        return [
            instrument.to_otlp()
            for instrument in [*self._counters.values(), *self._gauges.values(), *self._histograms.values()]
        ]

    def flush(self, exporter: Any) -> None:
        """Write current snapshot to *exporter* (a ``FileMetricsExporter``)."""
        data = self.snapshot()
        if data:
            exporter.export(data)
