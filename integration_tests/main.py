from collections.abc import Iterator, Mapping
import pathlib
import re
from typing import cast

from zahir.base_types import Context, Dependency, Job
from zahir.context import MemoryContext
from zahir.dependencies.group import DependencyGroup
from zahir.dependencies.job import JobDependency
from zahir.events import Await, JobOutputEvent, WorkflowOutputEvent
from zahir.job_registry import SQLiteJobRegistry
from zahir.scope import LocalScope
from zahir.tasks.decorator import job
from zahir.worker import LocalWorkflow

WORD_RE = re.compile(r"[^\W\d_]+(?:-[^\W\d_]+)*", re.UNICODE)


def longest_word_sequence(text: str) -> str:
    return cast(str, max(WORD_RE.findall(text), key=len, default=""))


def read_chapters(file_path: str) -> Iterator[list[str]]:
    chapter_lines: list[str] = []

    with pathlib.Path(file_path).open() as file:
        for line in file:
            if "CHAPTER" in line:
                if chapter_lines:
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


@job
def BookProcessor(
    cls, context: Context, input, dependencies
) -> Iterator[Job | JobOutputEvent]:

    pids = []
    chapters = read_chapters(input["file_path"])

    for chapter_lines in chapters:
        chapter_job = ChapterProcessor({"lines": chapter_lines}, {})
        yield chapter_job

        pids.append(chapter_job.job_id)

    assembly_deps: dict[str, list[Dependency]] = {
        "chapters": [JobDependency(pid, context.job_registry) for pid in pids]
    }

    # continue processing the job results, compute the list of longest words in the books
    yield LongestWordAssembly({}, assembly_deps)


@job
def ChapterProcessor(
    cls, context: Context, input, dependencies
) -> Iterator[JobOutputEvent]:
    """For each chapter, find the longest word."""

    # return the longest word found in the chapter
    yield JobOutputEvent({
        "longest_word": get_longest_word(input["lines"])
    })


@job
def LongestWordAssembly(
    cls, context: Context, input, dependencies
) -> Iterator[WorkflowOutputEvent]:
    """Assemble the longest words from each chapter into a unique list."""

    long_words = set()
    chapters = cast(list[JobDependency[Mapping]], dependencies.get("chapters"))

    for dep in chapters:
        summary = dep.output(context)
        if summary is not None:
            long_words.add(summary["longest_word"])

    yield WorkflowOutputEvent({"longest_words_by_chapter": sorted(long_words, key=len)})


scope = LocalScope(
    jobs=[BookProcessor, ChapterProcessor, LongestWordAssembly],
    dependencies=[DependencyGroup, JobDependency],
)

job_registry = SQLiteJobRegistry("jobs.db")
context = MemoryContext(scope=scope, job_registry=job_registry)

start = BookProcessor(
    {"file_path": "/home/rg/Code/zahir/integration_tests/data.txt"}, {}
)

for event in LocalWorkflow(context).run(start):
    print(event)
