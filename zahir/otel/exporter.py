"""File-based OTLP JSON span exporter."""

import json
import os
import pathlib
import socket
from typing import Any


class FileSpanExporter:
    """Exports spans to a file in OTLP JSON (one resource-span envelope per line)."""

    def __init__(self, file_path: pathlib.Path) -> None:
        self.file_path = file_path
        self.file_handle = file_path.open("a", encoding="utf-8", buffering=1)

    def export(self, span_data: dict[str, Any]) -> None:
        """Wrap *span_data* in an OTLP resource-span envelope and append to the file."""
        envelope = {
            "resourceSpans": [
                {
                    "resource": {
                        "attributes": [
                            {"key": "service.name", "value": {"stringValue": "zahir"}},
                            {"key": "service.instance.id", "value": {"stringValue": f"overseer-{os.getpid()}"}},
                            {"key": "host.name", "value": {"stringValue": socket.gethostname()}},
                            {"key": "process.pid", "value": {"intValue": str(os.getpid())}},
                        ],
                    },
                    "scopeSpans": [
                        {
                            "scope": {"name": "zahir"},
                            "spans": [span_data],
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
