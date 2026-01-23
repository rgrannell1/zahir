from collections.abc import Generator, Iterator
import pathlib
import re
from typing import Any, TypedDict, cast

from zahir.base_types import Context, JobInstance
from zahir.context import MemoryContext
from zahir.events import Await, JobOutputEvent, WorkflowOutputEvent, ZahirEvent
from zahir.job_registry import SQLiteJobRegistry
from zahir.jobs.decorator import spec
from zahir.jobs.sleep import Sleep
from zahir.scope import LocalScope
from zahir.worker import LocalWorkflow


class ChapterProcessorInput(TypedDict):
    lines: list[str]


class ChapterProcessorOutput(TypedDict):
    longest_word: str


class BookProcessorInput(TypedDict):
    file_path: str


class BookProcessorOutput(TypedDict):
    longest_words: list[str]


class UppercaseWordsInput(TypedDict):
    words: list[str]


class UppercaseWordsOutput(TypedDict):
    words: list[str]

WORD_RE = re.compile(r"[^\W\d_]+(?:-[^\W\d_]+)*", re.UNICODE)


def longest_word_sequence(text: str) -> str:
    return cast(str, max(WORD_RE.findall(text), key=len, default=""))


def read_chapters(file_path: str) -> Iterator[list[str]]:
    chapter_lines: list[str] = []

    with pathlib.Path(file_path).open(encoding="utf-8") as file:
        for line in file:
            if "CHAPTER" in line and chapter_lines:
                yield chapter_lines
                chapter_lines = []

            chapter_lines.append(line)

    if chapter_lines:
        yield chapter_lines


def get_longest_word(words: list[str]) -> str:
    longest_word = ""

    for line in words:
        for word in line.split():
            tidied_word = longest_word_sequence(word)

            if len(tidied_word) > len(longest_word):
                longest_word = tidied_word

    return longest_word


@spec(args=ChapterProcessorInput, output=ChapterProcessorOutput)
def ChapterProcessor(context, args, dependencies) -> Generator[JobOutputEvent]:
    """For each chapter, find the longest word."""

    # return the longest word found in the chapter
    yield JobOutputEvent({"longest_word": get_longest_word(args["lines"])})


@spec(args=BookProcessorInput, output=BookProcessorOutput)
def BookProcessor(
    context, args, dependencies
) -> Generator[Await | JobInstance | WorkflowOutputEvent, ChapterProcessorOutput | BookProcessorOutput]:
    longest_words = yield Await([
        ChapterProcessor({"lines": chapter_lines}, {}) for chapter_lines in read_chapters(args["file_path"])
    ])

    long_words: set[str] = set()

    for summary in longest_words:
        if summary is not None:
            long_words.add(summary["longest_word"])

    uppercased = yield Await(UppercaseWords({"words": list(long_words)}, {}))

    yield Await(Sleep({"duration_seconds": 2}, {}))

    yield WorkflowOutputEvent({"longest_words": uppercased["words"]})


@spec(args=UppercaseWordsInput, output=UppercaseWordsOutput)
def UppercaseWords(context: Context, args, dependencies) -> Generator[JobOutputEvent]:
    """Uppercase a list of words."""

    yield JobOutputEvent({"words": [word.upper() for word in args["words"]]})


job_registry = SQLiteJobRegistry("jobs.db")
context = MemoryContext(scope=LocalScope.from_module(), job_registry=job_registry)

start = BookProcessor({"file_path": "integration_tests/data.txt"}, {}) # type: ignore

# Traces are written to "traces/" by default
workflow = LocalWorkflow(context=context, max_workers=15) # type: ignore

events = list[WorkflowOutputEvent | ZahirEvent](workflow.run(start, events_filter=None))
print(events[-5:-1])
