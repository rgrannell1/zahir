import re

from typing import Iterator, Mapping, TypedDict, cast
from zahir.context import MemoryContext
from zahir.dependencies.group import DependencyGroup
from zahir.dependencies.job import JobDependency
from zahir.events import JobEvent, JobOutputEvent, WorkflowOutputEvent
from zahir.scope import LocalScope
from zahir.tasks.decorator import job
from zahir.base_types import Context, Job
from zahir.worker import zahir_worker_pool
from zahir.base_types import Dependency
from zahir.job_registry import SQLiteJobRegistry

WORD_RE = re.compile(r"[^\W\d_]+(?:-[^\W\d_]+)*", re.UNICODE)


def longest_word_sequence(text: str) -> str:
    from typing import cast

    return cast(str, max(WORD_RE.findall(text), key=len, default=""))


@job
def BookProcessor(
    cls, context: Context, input, dependencies
) -> Iterator[Job | JobOutputEvent]:
    pids = []
    chapter_lines: list[str] = []

    with open(input["file_path"], "r") as file:
        for line in file:
            if "CHAPTER" in line:
                chapter_job = ChapterProcessor({"lines": chapter_lines.copy()}, {})
                yield chapter_job
                pids.append(chapter_job.job_id)

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



# for event in workflow.run(
#    BookProcessor({"file_path": "/home/rg/Code/zahir/integration_tests/data.txt"}, {})
# ):
#    print(event.output)

db = SQLiteJobRegistry("jobs.db")
db.add(
    BookProcessor({"file_path": "/home/rg/Code/zahir/integration_tests/data.txt"}, {})
)

for event in zahir_worker_pool(scope, worker_count=4):
    print(event)
