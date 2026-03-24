"""Tests for zahir.otel.metrics and zahir.otel.metrics_exporter.

Validates counter, gauge, and histogram instruments produce correct
OTLP JSON structures, and that the FileMetricsExporter writes valid
JSONL that can be parsed back.
"""

import json
import pathlib
import tempfile

import pytest

from zahir.otel.metrics import MetricsCollector, _Counter, _Gauge, _Histogram
from zahir.otel.metrics_exporter import FileMetricsExporter


# ── Counter ──────────────────────────────────────────────────────


class TestCounter:
    def test_initial_value_is_zero(self):
        counter = _Counter("test.counter", "a test counter", "ops")
        assert counter.value == 0

    def test_add_increments(self):
        counter = _Counter("test.counter", "", "")
        counter.add(3)
        counter.add(7)
        assert counter.value == 10

    def test_add_default_is_one(self):
        counter = _Counter("test.counter", "", "")
        counter.add()
        counter.add()
        assert counter.value == 2

    def test_otlp_structure(self):
        counter = _Counter("jobs.completed", "total completed", "{jobs}")
        counter.add(42)
        otlp = counter.to_otlp()

        assert otlp["name"] == "jobs.completed"
        assert otlp["description"] == "total completed"
        assert otlp["unit"] == "{jobs}"

        data_point = otlp["sum"]["dataPoints"][0]
        assert data_point["asDouble"] == 42
        assert data_point["isMonotonic"] is True
        assert data_point["aggregationTemporality"] == 2
        assert int(data_point["startTimeUnixNano"]) > 0
        assert int(data_point["timeUnixNano"]) >= int(data_point["startTimeUnixNano"])


# ── Gauge ────────────────────────────────────────────────────────


class TestGauge:
    def test_initial_value_is_zero(self):
        gauge = _Gauge("test.gauge", "", "")
        assert gauge.value == 0

    def test_set_replaces_value(self):
        gauge = _Gauge("test.gauge", "", "")
        gauge.set(100)
        assert gauge.value == 100
        gauge.set(5)
        assert gauge.value == 5

    def test_otlp_structure(self):
        gauge = _Gauge("active.workflows", "how many", "{workflows}")
        gauge.set(3)
        otlp = gauge.to_otlp()

        assert otlp["name"] == "active.workflows"
        assert otlp["description"] == "how many"
        data_point = otlp["gauge"]["dataPoints"][0]
        assert data_point["asDouble"] == 3
        assert int(data_point["timeUnixNano"]) > 0


# ── Histogram ────────────────────────────────────────────────────


class TestHistogram:
    def test_empty_histogram(self):
        hist = _Histogram("test.hist", "", "", bounds=[10, 100])
        assert hist.count == 0
        assert hist.total == 0.0

    def test_single_record(self):
        hist = _Histogram("test.hist", "", "", bounds=[10, 100])
        hist.record(42)
        assert hist.count == 1
        assert hist.total == 42.0
        assert hist.minimum == 42
        assert hist.maximum == 42

    def test_bucket_assignment(self):
        hist = _Histogram("test.hist", "", "", bounds=[10, 50, 100])
        # 4 buckets: [0..10], (10..50], (50..100], (100..+inf)
        hist.record(5)    # bucket 0
        hist.record(10)   # bucket 0 (<=10)
        hist.record(50)   # bucket 1 (<=50)
        hist.record(75)   # bucket 2 (<=100)
        hist.record(200)  # bucket 3 (overflow)
        assert hist.bucket_counts == [2, 1, 1, 1]

    def test_all_overflow(self):
        hist = _Histogram("test.hist", "", "", bounds=[10])
        hist.record(100)
        hist.record(200)
        assert hist.bucket_counts == [0, 2]

    def test_all_underflow(self):
        hist = _Histogram("test.hist", "", "", bounds=[100])
        hist.record(1)
        hist.record(2)
        assert hist.bucket_counts == [2, 0]

    def test_min_max_tracking(self):
        hist = _Histogram("test.hist", "", "", bounds=[10, 100])
        hist.record(50)
        hist.record(3)
        hist.record(99)
        assert hist.minimum == 3
        assert hist.maximum == 99

    def test_otlp_structure(self):
        hist = _Histogram("loop.ms", "loop time", "ms", bounds=[10, 100])
        hist.record(5)
        hist.record(50)
        hist.record(200)
        otlp = hist.to_otlp()

        assert otlp["name"] == "loop.ms"
        data_point = otlp["histogram"]["dataPoints"][0]
        assert data_point["count"] == "3"
        assert data_point["sum"] == 255.0
        assert data_point["min"] == 5
        assert data_point["max"] == 200
        assert data_point["explicitBounds"] == [10, 100]
        assert data_point["bucketCounts"] == ["1", "1", "1"]
        assert data_point["aggregationTemporality"] == 2

    def test_empty_otlp_min_max_are_zero(self):
        hist = _Histogram("test.hist", "", "", bounds=[10])
        otlp = hist.to_otlp()
        data_point = otlp["histogram"]["dataPoints"][0]
        assert data_point["min"] == 0
        assert data_point["max"] == 0


# ── MetricsCollector ─────────────────────────────────────────────


class TestMetricsCollector:
    def test_counter_shortcut(self):
        collector = MetricsCollector()
        collector.counter("a", 5)
        collector.counter("a", 3)
        snapshot = collector.snapshot()
        counter_metric = [metric for metric in snapshot if metric["name"] == "a"][0]
        assert counter_metric["sum"]["dataPoints"][0]["asDouble"] == 8

    def test_gauge_shortcut(self):
        collector = MetricsCollector()
        collector.gauge("g", 42)
        snapshot = collector.snapshot()
        gauge_metric = [metric for metric in snapshot if metric["name"] == "g"][0]
        assert gauge_metric["gauge"]["dataPoints"][0]["asDouble"] == 42

    def test_histogram_shortcut(self):
        collector = MetricsCollector()
        collector.histogram("h", 10)
        collector.histogram("h", 20)
        snapshot = collector.snapshot()
        hist_metric = [metric for metric in snapshot if metric["name"] == "h"][0]
        assert hist_metric["histogram"]["dataPoints"][0]["count"] == "2"

    def test_get_returns_same_instance(self):
        collector = MetricsCollector()
        counter_a = collector.get_counter("x")
        counter_b = collector.get_counter("x")
        assert counter_a is counter_b

    def test_snapshot_contains_all_instrument_types(self):
        collector = MetricsCollector()
        collector.counter("c", 1)
        collector.gauge("g", 2)
        collector.histogram("h", 3)
        snapshot = collector.snapshot()
        names = {metric["name"] for metric in snapshot}
        assert names == {"c", "g", "h"}

    def test_snapshot_is_empty_initially(self):
        collector = MetricsCollector()
        assert collector.snapshot() == []


# ── FileMetricsExporter ─────────────────────────────────────────


class TestFileMetricsExporter:
    def test_export_writes_jsonl(self, tmp_path: pathlib.Path):
        filepath = tmp_path / "metrics.jsonl"
        exporter = FileMetricsExporter(filepath)

        collector = MetricsCollector()
        collector.counter("jobs", 10)
        collector.gauge("workers", 4)
        collector.flush(exporter)

        exporter.close()

        lines = filepath.read_text().strip().split("\n")
        assert len(lines) == 1

        envelope = json.loads(lines[0])
        assert "resourceMetrics" in envelope

        resource_metrics = envelope["resourceMetrics"][0]
        assert resource_metrics["resource"]["attributes"][0]["key"] == "service.name"

        scope_metrics = resource_metrics["scopeMetrics"][0]
        assert scope_metrics["scope"]["name"] == "zahir"
        metric_names = {metric["name"] for metric in scope_metrics["metrics"]}
        assert "jobs" in metric_names
        assert "workers" in metric_names

    def test_multiple_flushes_append(self, tmp_path: pathlib.Path):
        filepath = tmp_path / "metrics.jsonl"
        exporter = FileMetricsExporter(filepath)

        collector = MetricsCollector()
        collector.counter("a", 1)
        collector.flush(exporter)
        collector.counter("a", 1)
        collector.flush(exporter)

        exporter.close()

        lines = filepath.read_text().strip().split("\n")
        assert len(lines) == 2

        # Second line should show cumulative value of 2.
        envelope = json.loads(lines[1])
        metrics = envelope["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
        counter_metric = [metric for metric in metrics if metric["name"] == "a"][0]
        assert counter_metric["sum"]["dataPoints"][0]["asDouble"] == 2

    def test_empty_flush_writes_nothing(self, tmp_path: pathlib.Path):
        filepath = tmp_path / "metrics.jsonl"
        exporter = FileMetricsExporter(filepath)

        collector = MetricsCollector()
        collector.flush(exporter)  # nothing registered

        exporter.close()

        content = filepath.read_text()
        assert content == ""

    def test_all_lines_are_valid_json(self, tmp_path: pathlib.Path):
        filepath = tmp_path / "metrics.jsonl"
        exporter = FileMetricsExporter(filepath)

        collector = MetricsCollector()
        collector.counter("c", 1)
        collector.histogram("h", 50)
        collector.gauge("g", 99)
        for _ in range(5):
            collector.flush(exporter)

        exporter.close()

        for line in filepath.read_text().strip().split("\n"):
            parsed = json.loads(line)
            assert "resourceMetrics" in parsed
