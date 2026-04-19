from zahir.progress_bar.progress_bar_state import JobStats


def job_status(stats: JobStats) -> str:
    if stats.failed > 0:
        return "failed"
    if stats.total > 0 and stats.processed >= stats.total:
        return "success"
    return "running"


def job_description(fn_name: str, stats: JobStats) -> str:
    in_flight = stats.started - stats.processed
    parts = []

    if in_flight > 0:
        parts.append(f"{in_flight} running")
    if stats.completed > 0:
        parts.append(f"{stats.completed} done")
    if stats.failed > 0:
        parts.append(f"[red]{stats.failed} failed[/]")

    body = ", ".join(parts) or "starting"

    return f"  [blue]{fn_name}[/]: {body}"


def workflow_description(total: int, processed: int, eta: str) -> str:
    return f"Workflow | {processed}/{total} | eta {eta}"


def system_description(cores: int, cpu: float, ram: float) -> str:
    return f"Zahir | {cores} cores | cpu {cpu:.0f}% | ram {ram:.0f}%"
