"""Utility functions for computing stable hashes from job data."""

from collections.abc import Callable, Mapping
import hashlib
import json
from typing import Any


def compute_stable_hash(data: Mapping[str, Any]) -> str:
    hash_string = json.dumps(data, sort_keys=True)
    return hashlib.sha256(hash_string.encode("utf-8")).hexdigest()


def compute_idempotence_key(
    job_type: str,
    args: Mapping[str, Any],
    dependencies: Mapping[str, Any],
    once_by: Callable[[str, Mapping[str, Any], Mapping[str, Any]], str] | None = None,
) -> str:
    if once_by is not None:
        return once_by(job_type, args, dependencies)

    # Type + args by default, if once=True
    hash_data = {
        "type": job_type,
        "args": args,
    }
    return compute_stable_hash(hash_data)
