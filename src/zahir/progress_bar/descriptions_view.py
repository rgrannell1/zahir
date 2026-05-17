from zahir.progress_bar.dep_labels import short_label
from zahir.progress_bar.progress_bar_state_model import JobStats


def job_status(stats: JobStats) -> str:
    if stats.failed > 0:
        return "failed"
    if stats.total > 0 and stats.processed >= stats.total:
        return "success"
    return "running"


_MEAN_WIDTH = 10  # width of the μXs prefix column, e.g. "μ1.23s    "


def _format_mean(mean_ms: float | None) -> str:
    """Format mean duration as μXs, padded to a fixed width."""
    if mean_ms is None:
        return " " * _MEAN_WIDTH
    seconds = mean_ms / 1000

    return f"μ{seconds:.2f}s".ljust(_MEAN_WIDTH)


def _format_waiting(waiting: dict[str, int]) -> str:
    entries = ", ".join(f"{count} {short_label(dep)}" for dep, count in waiting.items())
    return f"[yellow](w: {entries})[/]"


def _job_status_parts(stats: JobStats, waiting: dict[str, int] | None) -> list[str]:
    """Build the list of status fragments for a job row."""

    running = stats.started - stats.processed
    parts = []

    if running > 0:
        running_str = f"{running} running"
        if waiting:
            running_str = f"{running_str} {_format_waiting(waiting)}"
        parts.append(running_str)

    if stats.total > stats.started:
        parts.append(f"{stats.total - stats.started} queued")
    if stats.completed > 0:
        parts.append(f"{stats.completed} done")
    if stats.failed > 0:
        parts.append(f"[red]{stats.failed} failed[/]")

    return parts


def job_description(fn_name: str, stats: JobStats, mean_ms: float | None = None, waiting: dict[str, int] | None = None) -> str:
    parts = _job_status_parts(stats, waiting)
    body = ", ".join(parts) or "starting"
    mean = _format_mean(mean_ms)
    return f"  {mean}[blue]{fn_name}[/]: {body}"


def workflow_description(elapsed: str, completed: int, remaining: int, eta: str) -> str:
    return f"Workflow | {elapsed} elapsed | {completed} done | {remaining} remaining | eta {eta}"


def system_description(cores: int, cpu: float, ram: float) -> str:
    return f"Zahir | {cores} cores | cpu {cpu:.0f}% | ram {ram:.0f}%"
