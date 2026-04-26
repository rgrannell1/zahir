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


def job_description(fn_name: str, stats: JobStats, mean_ms: float | None = None, waiting: dict[str, int] | None = None) -> str:
    in_flight = stats.started - stats.processed
    parts = []

    if in_flight > 0:
        running = f"{in_flight} running"
        if waiting:
            running = f"{running} {_format_waiting(waiting)}"
        parts.append(running)

    if stats.completed > 0:
        parts.append(f"{stats.completed} done")

    if stats.failed > 0:
        parts.append(f"[red]{stats.failed} failed[/]")

    body = ", ".join(parts) or "starting"
    mean = _format_mean(mean_ms)

    return f"  {mean}[blue]{fn_name}[/]: {body}"


def workflow_description(total: int, processed: int, eta: str) -> str:
    return f"Workflow | {processed}/{total} | eta {eta}"


def system_description(cores: int, cpu: float, ram: float) -> str:
    return f"Zahir | {cores} cores | cpu {cpu:.0f}% | ram {ram:.0f}%"
