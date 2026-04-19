from rich.progress import ProgressColumn
from rich.text import Text


_BAR_WIDTH = 28

_STATUS_STYLES = {
    "running": "cyan",
    "success": "green",
    "failed": "red",
}


def _bar_style(task) -> str:
    return _STATUS_STYLES.get(task.fields.get("status", "running"), "cyan")


def _filled_width(task) -> int:
    if not task.total:
        return 0
    ratio = min(task.completed / task.total, 1.0)
    return int(_BAR_WIDTH * ratio)


class JobBarColumn(ProgressColumn):
    """Coloured block progress bar. Colour reflects job status field."""

    def render(self, task) -> Text:
        if task.fields.get("hide_bar"):
            return Text("")

        filled = _filled_width(task)
        empty = _BAR_WIDTH - filled
        bar = "█" * filled + "░" * empty

        return Text(bar, style=_bar_style(task))


class NofMColumn(ProgressColumn):
    """Shows completed/total. Hidden for rows that set hide_bar."""

    def render(self, task) -> Text:
        if task.fields.get("hide_bar"):
            return Text("")
        return Text(f"{int(task.completed)}/{int(task.total or 0)}")
