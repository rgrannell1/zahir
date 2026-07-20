import pathlib
from collections.abc import Generator
from typing import Any

from zahir.core.evaluate import JobContext, evaluate, setup
from zahir.core.telemetry import make_telemetry
from zahir.progress_bar.progress_bar import with_progress

FILE_PATH = pathlib.Path(__file__).parent / "warandpeace.txt"
CHUNK_SIZE = 200  # lines per chapter


def _chunks(file_path: pathlib.Path, size: int) -> list[list[str]]:
    lines = file_path.read_text().splitlines()
    return [lines[idx : idx + size] for idx in range(0, len(lines), size)]


def _longest_word(lines: list[str]) -> str:
    words = " ".join(lines).split()
    return max(words, key=len) if words else ""


# The jobs


def chapter_processor(ctx: JobContext, lines: list[str]) -> Generator[Any, Any, str]:
    """Return the longest word in this chunk of lines."""
    yield from ()
    return _longest_word(lines)


def uppercase_words(ctx: JobContext, words: list[str]) -> Generator[Any, Any, list[str]]:
    """Return the uppercased words."""
    yield from ()
    return [word.upper() for word in words]


def book_processor(ctx: JobContext, file_path: str) -> Generator[Any, Any, dict]:
    """Fan out to chapter processors, collect longest words, uppercase, emit."""
    chunks = _chunks(pathlib.Path(file_path), CHUNK_SIZE)

    longest_words: set[str] = set()
    for chunk in chunks:
        longest: str = yield ctx.scope.chapter_processor(chunk)
        if longest:
            longest_words.add(longest)

    uppercased: list[str] = yield ctx.scope.uppercase_words(sorted(longest_words))
    return {"longest_words": uppercased}


_SCOPE = {
    "book_processor": book_processor,
    "chapter_processor": chapter_processor,
    "uppercase_words": uppercase_words,
}


def main():
    res = yield from with_progress(
        evaluate(
            setup(n_workers=4),
            "book_processor",
            (str(FILE_PATH),),
            scope=_SCOPE,
            handler_wrappers=[make_telemetry()],
        )
    )

    return res


if __name__ == "__main__":
    main()
