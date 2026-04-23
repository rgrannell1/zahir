import time
from collections.abc import Generator, Iterable
from typing import Any

from bookman.events import Event
from zahir.progress_bar.progress_bar_service import ProgressBarService
from zahir.progress_bar.rich_display_service import RichDisplayService

_POLL_INTERVAL_S = 0.25


def with_progress(events: Iterable[Any]) -> Generator[Any, None, None]:
    """Wrap any event iterable (e.g. evaluate(...)) and render a live progress bar.

    Telemetry events are consumed for display; all events are passed through unchanged.
    cpu/ram are polled at most once per second, independent of event rate.
    """
    bar = ZahirProgressBar()
    last_poll = time.monotonic()

    with bar:
        for event in events:
            now = time.monotonic()

            if now - last_poll >= _POLL_INTERVAL_S:
                bar.poll()
                last_poll = now

            if isinstance(event, Event):
                bar.update(event)

            yield event


class ZahirProgressBar:
    """Thin coordinator: owns a ProgressBarService and RichDisplayService."""

    def __init__(self):
        self._service = ProgressBarService()
        self._display = RichDisplayService()

    def __enter__(self):
        self._display.__enter__()
        return self

    def __exit__(self, *args):
        return self._display.__exit__(*args)

    def poll(self) -> None:
        """Sample cpu/ram from psutil. Call periodically from the main loop."""

        self._service.poll()
        self._display.refresh(self._service)

    def update(self, event: Event) -> None:
        """Feed a telemetry event into all state components and refresh the display."""

        self._service.update(event)
        self._display.refresh(self._service)
