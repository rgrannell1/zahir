#!/usr/bin/env -S uv run python3
"""Send zahir metrics JSONL files to an OTLP HTTP endpoint.

Usage::

    uv run python3 bs/send-metrics.py
    uv run python3 bs/send-metrics.py --traces-dir traces/ --otlp-endpoint http://localhost:4318
"""

import json
import pathlib
import sys

import httpx


def send_metrics_file(file_path: pathlib.Path, otlp_endpoint: str) -> None:
    """Send a single metrics JSONL file to the OTLP endpoint."""
    print(f"Sending metrics from {file_path}...")

    sent = 0
    errors = 0
    with file_path.open(encoding="utf-8") as handle:
        for line_num, line in enumerate(handle, 1):
            line = line.strip()
            if not line:
                continue

            try:
                resource_metrics = json.loads(line)
                response = httpx.post(
                    f"{otlp_endpoint}/v1/metrics",
                    json=resource_metrics,
                    headers={"Content-Type": "application/json"},
                    timeout=10.0,
                )
                response.raise_for_status()
                sent += 1
            except json.JSONDecodeError as err:
                print(f"  JSON error line {line_num}: {err}", file=sys.stderr)
                errors += 1
            except httpx.HTTPError as err:
                print(f"  HTTP error line {line_num}: {err}", file=sys.stderr)
                errors += 1

    print(f"  sent {sent} envelopes, {errors} errors")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Send zahir metrics JSONL to OTLP endpoint")
    parser.add_argument(
        "--traces-dir",
        type=pathlib.Path,
        default=pathlib.Path("traces"),
        help="Directory containing metrics-*.jsonl files",
    )
    parser.add_argument(
        "--otlp-endpoint",
        default="http://localhost:4318",
        help="OTLP HTTP endpoint URL",
    )
    parser.add_argument(
        "--file",
        type=pathlib.Path,
        help="Send a specific metrics file",
    )

    args = parser.parse_args()

    if args.file:
        if not args.file.exists():
            print(f"Error: {args.file} does not exist", file=sys.stderr)
            sys.exit(1)
        send_metrics_file(args.file, args.otlp_endpoint)
    else:
        if not args.traces_dir.exists():
            print(f"Error: {args.traces_dir} does not exist", file=sys.stderr)
            sys.exit(1)

        metrics_files = sorted(args.traces_dir.glob("metrics-*.jsonl"))
        if not metrics_files:
            print(f"No metrics-*.jsonl files found in {args.traces_dir}", file=sys.stderr)
            sys.exit(1)

        print(f"Found {len(metrics_files)} metrics file(s)")
        for metrics_file in metrics_files:
            send_metrics_file(metrics_file, args.otlp_endpoint)

    print("Done!")


if __name__ == "__main__":
    main()
