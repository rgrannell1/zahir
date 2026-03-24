"""File-based OTLP JSON metrics exporter.

Mirrors the approach in ``exporter.py`` (spans), but for metrics.  Each
call to ``export`` appends one ``resourceMetrics`` envelope as a JSON
line, keeping the files easy to process with the same tools that consume
trace JSONL.
"""

import json
import os
import pathlib
import socket
from typing import Any


class FileMetricsExporter:
    """Writes OTLP-formatted metric data-points to a JSONL file."""

    def __init__(self, file_path: pathlib.Path) -> None:
        self.file_path = file_path
        self.file_handle = file_path.open("a", encoding="utf-8", buffering=1)

    def export(self, metrics: list[dict[str, Any]]) -> None:
        """Wrap *metrics* in an OTLP ``resourceMetrics`` envelope and append."""
        if not metrics:
            return

        envelope = {
            "resourceMetrics": [
                {
                    "resource": {
                        "attributes": [
                            {"key": "service.name", "value": {"stringValue": "zahir"}},
                            {"key": "service.instance.id", "value": {"stringValue": f"overseer-{os.getpid()}"}},
                            {"key": "host.name", "value": {"stringValue": socket.gethostname()}},
                            {"key": "process.pid", "value": {"intValue": str(os.getpid())}},
                        ],
                    },
                    "scopeMetrics": [
                        {
                            "scope": {"name": "zahir"},
                            "metrics": metrics,
                        },
                    ],
                },
            ],
        }
        self.file_handle.write(json.dumps(envelope) + "\n")
        self.file_handle.flush()

    def close(self) -> None:
        """Close the underlying file handle."""
        if self.file_handle:
            self.file_handle.close()
