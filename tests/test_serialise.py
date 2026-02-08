"""Tests for serialise module - event serialization and deserialization."""

import pytest

from zahir.base_types import Context
from zahir.context import MemoryContext
from zahir.events import JobOutputEvent, WorkflowOutputEvent, ZahirCustomEvent, ZahirEvent, check_output_json_serialisable
from zahir.scope import LocalScope
from zahir.serialise import Serialisable, SerialisedEvent, deserialise_event, serialise_event


class NonSerialisableEvent:
    """A class that doesn't implement Serialisable protocol."""

    def __init__(self, value: str):
        self.value = value


class MockSerialisableEvent:
    """A mock event that implements Serialisable but isn't a ZahirEvent."""

    def __init__(self, value: str):
        self.value = value

    def save(self, context: Context) -> dict:
        """Save method for Serialisable protocol."""
        return {"value": self.value}

    @classmethod
    def load(cls, context: Context, data: dict):
        """Load method for Serialisable protocol."""
        return cls(data["value"])


def test_job_output_event_rejects_non_json_serializable_output():
    """JobOutputEvent must be given JSON-serializable output (stored as JSON)."""
    with pytest.raises(TypeError) as exc_info:
        JobOutputEvent(output={"triple": object()})  # object() is not JSON-serializable
    assert "JobOutputEvent output must be JSON-serializable" in str(exc_info.value)
    assert "object" in str(exc_info.value).lower() or "Object" in str(exc_info.value)


def test_check_output_json_serializable_accepts_dict_and_list():
    """check_output_json_serializable accepts dicts and lists."""
    check_output_json_serialisable({"a": 1, "b": [2, 3]})
    check_output_json_serialisable([])
    check_output_json_serialisable({})


def test_workflow_output_event_rejects_non_json_serialisable_output():
    """WorkflowOutputEvent must be given JSON-serialisable output."""
    with pytest.raises(TypeError) as exc_info:
        WorkflowOutputEvent(output={"gen": (x for x in range(3))})
    assert "WorkflowOutputEvent output must be JSON-serializable" in str(exc_info.value)


def test_workflow_output_event_rejects_generator():
    """WorkflowOutputEvent rejects a bare generator — the exact mistake that causes 'cannot pickle generator'."""
    def make_gen():
        yield 1

    with pytest.raises(TypeError) as exc_info:
        WorkflowOutputEvent(output={"items": make_gen()})
    assert "WorkflowOutputEvent" in str(exc_info.value)
    assert "JSON-serializable" in str(exc_info.value)


def test_zahir_custom_event_rejects_non_json_serialisable_output():
    """ZahirCustomEvent must be given JSON-serialisable output."""
    with pytest.raises(TypeError) as exc_info:
        ZahirCustomEvent(output={"bad": object()})
    assert "ZahirCustomEvent output must be JSON-serializable" in str(exc_info.value)


def test_zahir_custom_event_allows_none_output():
    """ZahirCustomEvent allows None output (it's optional)."""
    event = ZahirCustomEvent(output=None)
    assert event.output is None


def test_serialise_event_with_valid_event(simple_context):
    """Test serialise_event with a valid Serialisable event."""
    event = JobOutputEvent(output={"result": "success"})
    serialised = serialise_event(simple_context, event)

    assert isinstance(serialised, SerialisedEvent)
    assert serialised["type"] == "JobOutputEvent"
    assert "data" in serialised
    assert serialised["data"]["output"] == {"result": "success"}


def test_serialise_event_raises_typeerror_for_non_serialisable(simple_context):
    """Test serialise_event raises TypeError for non-Serialisable objects."""
    non_serialisable = NonSerialisableEvent("test")

    with pytest.raises(TypeError) as exc_info:
        serialise_event(simple_context, non_serialisable)

    assert "does not implement the Serialisable protocol" in str(exc_info.value)
    assert "NonSerialisableEvent" in str(exc_info.value)


def test_serialise_event_raises_typeerror_for_missing_save_method(simple_context):
    """Test serialise_event raises TypeError when save method is missing."""

    class NoSaveMethod:
        pass

    obj = NoSaveMethod()

    with pytest.raises(TypeError) as exc_info:
        serialise_event(simple_context, obj)

    assert "does not implement the Serialisable protocol" in str(exc_info.value)


def test_deserialise_event_with_valid_data(simple_context):
    """Test deserialise_event with valid serialised event data."""
    # First serialise an event
    original_event = JobOutputEvent(output={"test": "data"})
    serialised = serialise_event(simple_context, original_event)

    # Then deserialise it
    deserialised = deserialise_event(simple_context, serialised)

    assert isinstance(deserialised, JobOutputEvent)
    assert deserialised.output == {"test": "data"}


def test_deserialise_event_raises_valueerror_missing_type_key(simple_context):
    """Test deserialise_event raises ValueError when 'type' key is missing."""
    invalid_data = SerialisedEvent({"data": {"output": {"test": "value"}}})

    with pytest.raises(ValueError) as exc_info:
        deserialise_event(simple_context, invalid_data)

    assert "missing 'type' key" in str(exc_info.value)


def test_deserialise_event_raises_valueerror_missing_data_key(simple_context):
    """Test deserialise_event raises ValueError when 'data' key is missing."""
    invalid_data = SerialisedEvent({"type": "JobOutputEvent"})

    with pytest.raises(ValueError) as exc_info:
        deserialise_event(simple_context, invalid_data)

    assert "missing 'data' key" in str(exc_info.value)


def test_deserialise_event_raises_keyerror_unknown_event_type(simple_context):
    """Test deserialise_event raises KeyError for unknown event types."""
    invalid_data = SerialisedEvent({"type": "NonExistentEventType", "data": {"output": {"test": "value"}}})

    with pytest.raises(KeyError) as exc_info:
        deserialise_event(simple_context, invalid_data)

    assert "Unknown event type" in str(exc_info.value)
    assert "NonExistentEventType" in str(exc_info.value)


def test_deserialise_event_raises_keyerror_empty_event_type(simple_context):
    """Test deserialise_event raises KeyError for empty event type."""
    invalid_data = SerialisedEvent({"type": "", "data": {"output": {"test": "value"}}})

    with pytest.raises(KeyError) as exc_info:
        deserialise_event(simple_context, invalid_data)

    assert "Unknown event type" in str(exc_info.value)


def test_serialise_deserialise_roundtrip(simple_context):
    """Test roundtrip serialization and deserialization."""
    original = JobOutputEvent(output={"roundtrip": "test", "number": 42})

    serialised = serialise_event(simple_context, original)
    deserialised = deserialise_event(simple_context, serialised)

    assert isinstance(deserialised, JobOutputEvent)
    assert deserialised.output == original.output


def test_serialised_event_is_dict_subclass():
    """Test that SerialisedEvent is a dict subclass."""
    event = SerialisedEvent({"type": "Test", "data": {}})
    assert isinstance(event, dict)
    assert event["type"] == "Test"
    assert event["data"] == {}


def test_serialise_event_with_mock_serialisable(simple_context):
    """Test serialise_event works with objects that implement Serialisable but aren't ZahirEvents."""
    # This tests the isinstance check for Serialisable protocol
    mock_event = MockSerialisableEvent("test_value")
    serialised = serialise_event(simple_context, mock_event)

    assert serialised["type"] == "MockSerialisableEvent"
    assert serialised["data"]["value"] == "test_value"
