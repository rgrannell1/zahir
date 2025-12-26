"""Typed wrapper for ID generation."""

from coolname import generate_slug  # type: ignore[import-untyped]


def generate_id(num_words: int = 2) -> str:
    """Generate a unique slug-style identifier.
    
    @param num_words: Number of words to include in the slug
    @return: A hyphenated slug string (e.g., "purple-elephant")
    """
    return generate_slug(num_words)
