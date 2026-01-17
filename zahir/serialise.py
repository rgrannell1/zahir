
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from zahir.base_types import Context
    from zahir.events import ZahirEvent


@runtime_checkable
class Serialisable(Protocol):
    """Protocol for objects that can be serialised across process boundaries.

    All events passed through multiprocessing queues must implement this protocol.
    """

    def save(self, context: "Context") -> Mapping[str, Any]:
        """Serialize the object to a dictionary.

        @param context: The context for serialization
        @return: The serialized data
        """
        ...

    @classmethod
    def load(cls, context: "Context", data: Mapping[str, Any]) -> "Serialisable":
        """Deserialize the object from a dictionary.

        @param context: The context for deserialization
        @param data: The serialized data
        @return: The deserialized object
        """
        ...


# Registry of event types by name for deserialisation
_EVENT_REGISTRY: dict[str, type["ZahirEvent"]] = {}


def _get_all_subclasses(cls: type) -> list[type]:
    """Recursively get all subclasses of a class."""
    subclasses = []
    for subclass in cls.__subclasses__():
        subclasses.append(subclass)
        subclasses.extend(_get_all_subclasses(subclass))
    return subclasses


def _populate_event_registry() -> None:
    """Populate the event registry with all ZahirEvent subclasses.

    Uses __subclasses__() to automatically discover all event types,
    eliminating the need for manual registration.
    """
    global _EVENT_REGISTRY

    if _EVENT_REGISTRY:
        return  # Already populated

    from zahir.events import ZahirEvent

    # Auto-discover all ZahirEvent subclasses
    for cls in _get_all_subclasses(ZahirEvent):
        _EVENT_REGISTRY[cls.__name__] = cls


class SerialisedEvent(dict):
    """A serialised event wrapper that includes type information."""

    pass


def serialise_event(context: "Context", event: "ZahirEvent") -> SerialisedEvent:
    """Serialise an event for transmission across process boundaries.

    @param context: The context for serialization
    @param event: The event to serialise
    @return: A dictionary containing the event type and serialised data
    @raises TypeError: If the event doesn't implement the Serialisable protocol
    """
    if not isinstance(event, Serialisable):
        raise TypeError(
            f"Event {type(event).__name__} does not implement the Serialisable protocol. "
            f"Ensure it has save(context) and load(context, data) methods."
        )

    return SerialisedEvent({
        "type": type(event).__name__,
        "data": event.save(context),
    })


def deserialise_event(context: "Context", data: SerialisedEvent) -> "ZahirEvent":
    """Deserialise an event from its serialised form.

    @param context: The context for deserialization
    @param data: The serialised event data containing 'type' and 'data' keys
    @return: The deserialised event
    @raises KeyError: If the event type is not in the registry
    @raises ValueError: If the data is missing required keys
    """
    _populate_event_registry()

    if "type" not in data:
        raise ValueError("Serialised event data missing 'type' key")
    if "data" not in data:
        raise ValueError("Serialised event data missing 'data' key")

    event_type = data["type"]
    event_data = data["data"]

    if event_type not in _EVENT_REGISTRY:
        raise KeyError(
            f"Unknown event type '{event_type}'. "
            f"Ensure the event class is registered in the event registry."
        )

    event_class = _EVENT_REGISTRY[event_type]
    return event_class.load(context, event_data)
