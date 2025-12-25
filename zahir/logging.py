from zahir.types import EventRegistry, JobRegistry


class ZahirLogger:
    def __init__(
        self, event_registry: "EventRegistry", job_registry: "JobRegistry"
    ) -> None:
        self.event_registry = event_registry
        self.job_registry = job_registry

    def render(self, context: "Context") -> None:
        for job in self.job_registry.jobs(context):
            ...
