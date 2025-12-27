from zahir.events import ZahirEvent
from zahir.base_types import EventRegistry


class MemoryEventRegistry(EventRegistry):
    """Keep track of workflow events in memory."""

    events: list[ZahirEvent]

    def __init__(self) -> None:
        self.events = []

    def register(self, event: ZahirEvent) -> None:
        """Register an event in the event registry."""

        self.events.append(event)
