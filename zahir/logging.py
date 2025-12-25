from zahir.types import EventRegistry


class ZahirLogger:
    def __init__(self, event_registry: "EventRegistry") -> None:
        self.event_registry = event_registry

    def render(self) -> None: ...
