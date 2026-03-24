"""Span construction helpers (pure data, no I/O)."""

from datetime import datetime
from typing import Any

from zahir.otel.attributes import to_otlp_attribute, to_otlp_attributes
from zahir.utils.id_generator import generate_span_id

# OTLP status codes
STATUS_OK = 1
STATUS_ERROR = 2


def nanoseconds_since_epoch(timestamp: datetime | None) -> int:
    """Convert a datetime to nanoseconds since the Unix epoch.

    @param timestamp: The datetime to convert (or None for 0)
    @return: Nanoseconds since epoch
    """
    if timestamp is None:
        return 0
    return int(timestamp.timestamp() * 1_000_000_000)


def build_span(
    trace_id: str,
    span_id: str,
    name: str,
    start_time: datetime,
    parent_span_id: str | None = None,
    attributes: dict[str, Any] | None = None,
    links: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a span dict in OTLP format.

    @param trace_id: The trace ID
    @param span_id: The span ID
    @param name: The span name
    @param start_time: The span start time
    @param parent_span_id: Optional parent span ID
    @param attributes: Optional key-value attributes (will be converted to OTLP format)
    @param links: Optional span links for cross-span dependencies
    @return: The OTLP span dict
    """
    span: dict[str, Any] = {
        "traceId": trace_id,
        "spanId": span_id,
        "name": name,
        "kind": 1,  # SPAN_KIND_INTERNAL
        "startTimeUnixNano": nanoseconds_since_epoch(start_time),
        "attributes": to_otlp_attributes(attributes or {}),
    }

    if parent_span_id:
        span["parentSpanId"] = parent_span_id

    if links:
        span["links"] = links

    return span


def new_span(
    trace_id: str,
    name: str,
    start_time: datetime,
    parent_span_id: str | None = None,
    attributes: dict[str, Any] | None = None,
    links: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Convenience wrapper: build a span with a freshly generated span ID.

    @return: The OTLP span dict (with a new spanId)
    """
    return build_span(
        trace_id=trace_id,
        span_id=generate_span_id(),
        name=name,
        start_time=start_time,
        parent_span_id=parent_span_id,
        attributes=attributes,
        links=links,
    )


def finalise_span(
    span: dict[str, Any],
    end_time: datetime,
    status_code: int = STATUS_OK,
    status_message: str | None = None,
) -> None:
    """Mark a span as finished (mutates *span* in place).

    @param span: The span to finalise
    @param end_time: When the span ended
    @param status_code: STATUS_OK (1) or STATUS_ERROR (2)
    @param status_message: Optional human-readable status message
    """
    span["endTimeUnixNano"] = nanoseconds_since_epoch(end_time)
    status: dict[str, Any] = {"code": status_code}
    if status_message:
        status["message"] = status_message
    span["status"] = status


def add_attribute(span: dict[str, Any], key: str, value: Any) -> None:
    """Append a single OTLP attribute to a span (mutates *span* in place).

    @param span: The span to modify
    @param key: Attribute key
    @param value: Attribute value (None values are skipped)
    """
    attr = to_otlp_attribute(key, value)
    if attr is not None:
        span.setdefault("attributes", []).append(attr)
