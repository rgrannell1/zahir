import sys
from zahir.types import EventRegistry, JobInformation, JobRegistry, JobState


def clear_screen() -> None:
    sys.stdout.write("\033[2J\033[H")
    sys.stdout.flush()

class ZahirLogger:
    def __init__(
        self, event_registry: "EventRegistry", job_registry: "JobRegistry"
    ) -> None:
        self.event_registry = event_registry
        self.job_registry = job_registry

    @classmethod
    def task_emoji(cls, job_info: "JobInformation") -> str:
        """Get an emoji representing the job state.

        @param job_info: The job information
        @return: An emoji string
        """

        state_emoji_map = {
            "PENDING": "⏳",
            "RUNNING": "🔜",
            "COMPLETED": "✅",
            "FAILED": "❌",
            "CANCELLED": "🚫",
        }

        return state_emoji_map.get(job_info.state.name, "❓")

    def render(self, context: "Context") -> None:
        info: list[JobInformation] = []

        for job in self.job_registry.jobs(context):
            info.append(job)

        info.sort(key=lambda x: x.job_id)

        clear_screen()
        for job_info in info:
            emoji = self.task_emoji(job_info)

            message = f'{emoji} {job_info.job_id} ({job_info.job.__class__.__name__})'

            if job_info.state == JobState.COMPLETED and job_info.duration_seconds is not None:
                message += f' {job_info.duration_seconds:.2f}s'
            print(message)
