#!/usr/bin/env -S uv run python3
"""Send OpenTelemetry trace files to an OTLP endpoint."""

import json
import pathlib
import sys

import httpx


def send_trace_file(file_path: pathlib.Path, otlp_endpoint: str) -> None:
    """Send a single trace file to the OTLP endpoint."""
    print(f"Sending traces from {file_path}...")

    with file_path.open(encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            try:
                resource_spans = json.loads(line)
                response = httpx.post(
                    f"{otlp_endpoint}/v1/traces",
                    json=resource_spans,
                    headers={"Content-Type": "application/json"},
                    timeout=10.0,
                )
                response.raise_for_status()
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON on line {line_num} of {file_path}: {e}", file=sys.stderr)
            except httpx.HTTPError as e:
                print(f"Error sending trace on line {line_num}: {e}", file=sys.stderr)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Send OpenTelemetry trace files to OTLP endpoint")
    parser.add_argument(
        "--traces-dir",
        type=pathlib.Path,
        default=pathlib.Path("traces"),
        help="Directory containing trace files",
    )
    parser.add_argument(
        "--otlp-endpoint",
        default="http://localhost:4318",
        help="OTLP HTTP endpoint URL",
    )
    parser.add_argument(
        "--file",
        type=pathlib.Path,
        help="Send a specific trace file (optional)",
    )

    args = parser.parse_args()

    if args.file:
        if not args.file.exists():
            print(f"Error: File {args.file} does not exist", file=sys.stderr)
            sys.exit(1)
        send_trace_file(args.file, args.otlp_endpoint)
    else:
        if not args.traces_dir.exists():
            print(f"Error: Directory {args.traces_dir} does not exist", file=sys.stderr)
            sys.exit(1)

        trace_files = sorted(args.traces_dir.glob("traces-*.jsonl"))
        if not trace_files:
            print(f"No trace files found in {args.traces_dir}", file=sys.stderr)
            sys.exit(1)

        print(f"Found {len(trace_files)} trace file(s)")
        for trace_file in trace_files:
            send_trace_file(trace_file, args.otlp_endpoint)

    print("Done!")


if __name__ == "__main__":
    main()
