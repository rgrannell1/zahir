"""OTLP attribute formatting utilities."""

from typing import Any

OtlpAttribute = dict[str, Any]


def to_otlp_attribute(key: str, value: Any) -> OtlpAttribute | None:
    """Convert a key-value pair to an OTLP attribute dict.

    @param key: The attribute key
    @param value: The attribute value
    @return: The OTLP attribute dict, or None if the value is None
    """
    if value is None:
        return None

    if isinstance(value, bool):
        return {"key": key, "value": {"boolValue": value}}
    if isinstance(value, int):
        return {"key": key, "value": {"intValue": str(value)}}
    if isinstance(value, float):
        return {"key": key, "value": {"doubleValue": value}}
    if isinstance(value, str):
        return {"key": key, "value": {"stringValue": value}}

    return {"key": key, "value": {"stringValue": str(value)}}


def to_otlp_attributes(attrs: dict[str, Any]) -> list[OtlpAttribute]:
    """Convert a dict of key-value pairs to a list of OTLP attributes.

    Skips keys whose value is None.

    @param attrs: The attributes to convert
    @return: List of OTLP attribute dicts
    """
    result: list[OtlpAttribute] = []
    for key, value in attrs.items():
        attr = to_otlp_attribute(key, value)
        if attr is not None:
            result.append(attr)
    return result
