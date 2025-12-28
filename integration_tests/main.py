from collections.abc import Iterator, Mapping
import pathlib
import re
from typing import TypedDict, cast

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
    from typing import cast

    return cast(str, max(WORD_RE.findall(text), key=len, default=""))


@job
def BookProcessor(
    cls, context: Context, input, dependencies
) -> Iterator[Job | Await | JobOutputEvent]:
    pids = []
    chapter_lines: list[str] = []

    with pathlib.Path(input["file_path"]).open() as file:
        for line in file:
            if "CHAPTER" in line:
                chapter_job = ChapterProcessor({"lines": chapter_lines.copy()}, {})
                yield chapter_job
                pids.append(chapter_job.job_id)

                just_for_testing = yield Await(ChapterProcessor({"lines": chapter_lines.copy()}, {}))
                print('just_for_testing', just_for_testing)

                chapter_lines = []

            chapter_lines.append(line)

    agg_dependencies: dict[str, list[Dependency]] = {
        "chapters": [JobDependency(pid, context.job_registry) for pid in pids]
    }

    # await the job results, compute the list of longest words in the books
    yield LongestWordAssembly({}, agg_dependencies)


class ChapterProcessorOutput(TypedDict):
    top_shelf_word: str


@job
def ChapterProcessor(
    cls, context: Context, input, dependencies
) -> Iterator[JobOutputEvent[ChapterProcessorOutput]]:
    """For each chapter, find the longest word."""
    longest_word = ""

    for line in input["lines"]:
        for word in line.split():
            tidied_word = longest_word_sequence(word)

            if len(tidied_word) > len(longest_word):
                longest_word = tidied_word

    # return the longest word found in the chapter
    yield JobOutputEvent(cast(ChapterProcessorOutput, {"top_shelf_word": longest_word}))


class LongestWordAssemblyOutput(TypedDict):
    the_list: list[str]


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
            long_words.add(summary["top_shelf_word"])

    yield WorkflowOutputEvent(
        cast(LongestWordAssemblyOutput, {"the_list": sorted(long_words, key=len)})
    )


scope = LocalScope(
    jobs=[BookProcessor, ChapterProcessor, LongestWordAssembly],
    dependencies=[DependencyGroup, JobDependency],
)

job_registry = SQLiteJobRegistry("jobs.db")
context = MemoryContext(scope=scope, job_registry=job_registry)

start = BookProcessor(
    {"file_path": "/home/rg/Code/zahir/integration_tests/data.txt"}, {}
)

for event in LocalWorkflow[LongestWordAssemblyOutput](context).run(start):
    print(event)
