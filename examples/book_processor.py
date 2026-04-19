"""Book processor — find the longest word in each chapter of War and Peace.

Mirrors the zahir BookProcessor example:
  - book_processor fans out to one chapter_processor per chunk
  - chapter_processors return their longest word to the parent
  - book_processor collects, deduplicates, then awaits uppercase_words
  - final result is emitted as a stream event
"""

import pathlib
from collections.abc import Generator
from typing import Any

from tertius import EEmit

from evaluate import JobContext, evaluate


FILE_PATH = pathlib.Path(__file__).parent / "warandpeace.txt"
CHUNK_SIZE = 200  # lines per chapter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _chunks(file_path: pathlib.Path, size: int) -> list[list[str]]:
    lines = file_path.read_text().splitlines()
    return [lines[i : i + size] for i in range(0, len(lines), size)]


def _longest_word(lines: list[str]) -> str:
    words = " ".join(lines).split()
    return max(words, key=len) if words else ""


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------


def chapter_processor(ctx: JobContext, lines: list[str]) -> Generator[Any, Any, str]:
    """Return the longest word in this chunk of lines."""
    return _longest_word(lines)
    yield


def uppercase_words(
    ctx: JobContext, words: list[str]
) -> Generator[Any, Any, list[str]]:
    """Return the uppercased words."""
    return [word.upper() for word in words]
    yield


def book_processor(ctx: JobContext, file_path: str) -> Generator[Any, Any, None]:
    """Fan out to chapter processors, collect longest words, uppercase, emit."""
    chunks = _chunks(pathlib.Path(file_path), CHUNK_SIZE)

    longest_words: set[str] = set()
    for chunk in chunks:
        longest: str = yield ctx.scope.chapter_processor(chunk)
        if longest:
            longest_words.add(longest)

    uppercased: list[str] = yield ctx.scope.uppercase_words(sorted(longest_words))
    yield EEmit({"longest_words": uppercased})


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

_SCOPE = {
    "book_processor": book_processor,
    "chapter_processor": chapter_processor,
    "uppercase_words": uppercase_words,
}

if __name__ == "__main__":
    for event in evaluate(
        "book_processor", (str(FILE_PATH),), scope=_SCOPE, n_workers=4
    ):
        print(event)
