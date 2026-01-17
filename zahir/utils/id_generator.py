"""Typed wrapper for ID generation."""

import secrets
import string

from coolname import generate_slug  # type: ignore[import-untyped]


def generate_id() -> str:
    """Generate a unique slug-style identifier with a random suffix.

    @return: A hyphenated slug string with a 10-letter random suffix
            (e.g., "purple-elephant-abcdefghij")
    """
    random_suffix = "".join(secrets.choice(string.ascii_lowercase) for _ in range(10))
    return generate_slug(2) + "-" + random_suffix
