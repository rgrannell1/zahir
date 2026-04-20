import pathlib
from collections.abc import Generator
from typing import Any

from tertius import EEmit

from zahir.core.evaluate import JobContext, evaluate
from zahir.progress_bar.progress_bar import with_progress
from zahir.progress_bar.telemetry import make_telemetry


FILE_PATH = pathlib.Path(__file__).parent / "warandpeace.txt"
CHUNK_SIZE = 200  # lines per chapter


def _chunks(file_path: pathlib.Path, size: int) -> list[list[str]]:
    lines = file_path.read_text().splitlines()
    return [lines[i : i + size] for i in range(0, len(lines), size)]


def _longest_word(lines: list[str]) -> str:
    words = " ".join(lines).split()
    return max(words, key=len) if words else ""


# The jobs


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


_SCOPE = {
    "book_processor": book_processor,
    "chapter_processor": chapter_processor,
    "uppercase_words": uppercase_words,
}


class BookContext(JobContext):
    handler_wrappers = [make_telemetry()]


if __name__ == "__main__":
    for _ in with_progress(
        evaluate(
            "book_processor",
            (str(FILE_PATH),),
            scope=_SCOPE,
            n_workers=4,
            context=BookContext,
        )
    ):
        pass
