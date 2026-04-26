# Short display labels for known dependency types shown in the progress bar
from typing import Final

# Maps dep label prefixes (as produced by the dependency combinators) to short display names.
# Prefix matching is used so labels like "memory resource" or "sqlite './foo.db'" both resolve.
DEP_SHORT_LABELS: Final[list[tuple[str, str]]] = [
    ("memory resource", "MEM"),
    ("cpu resource",    "CPU"),
    ("concurrency slot", "LCK"),
    ("concurrency:",    "LCK"),
    ("semaphore:",      "SEM"),
    ("semaphore '",     "SEM"),
    ("sqlite '",        "SQL"),
    ("time",            "TIME"),
]


def short_label(dep: str) -> str:
    """Return the short display label for a dep string, or the dep string itself if unknown."""
    for prefix, label in DEP_SHORT_LABELS:
        if dep.startswith(prefix):
            return label
    return dep
