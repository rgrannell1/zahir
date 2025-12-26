import re
from typing import Iterator
from zahir.context import MemoryContext
from zahir.dependencies.job import JobDependency
from zahir.events import JobOutputEvent, WorkflowOutputEvent
from zahir.scope import LocalScope
from zahir.types import Context, Job
from zahir.workflow import Workflow

WORD_RE = re.compile(r"[^\W\d_]+(?:-[^\W\d_]+)*", re.UNICODE)

def longest_word_sequence(text: str) -> str:
    from typing import cast
    return cast(str, max(WORD_RE.findall(text), key=len, default=""))

class BookProcessor(Job):
    """Top-level job; splits the book into chapters and processes each, then aggregates results."""

    @classmethod
    def run(
        cls, context: Context, input, dependencies
    ) -> Iterator[Job | JobOutputEvent | WorkflowOutputEvent]:
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

        agg_dependencies = {
            "chapters": [JobDependency(pid, context.job_registry) for pid in pids]
        }

        # await the job results, compute the list of longest words in the books
        yield LongestWordAssembly({}, agg_dependencies)

class ChapterProcessor(Job):
    """For each chapter, find the longest word."""

    @classmethod
    def run(
        cls, context: Context, input, dependencies
    ) -> Iterator[Job | JobOutputEvent | WorkflowOutputEvent]:
        longest_word = ""

        for line in input["lines"]:
            for word in line.split():
                tidied_word = longest_word_sequence(word)

                if len(tidied_word) > len(longest_word):
                    longest_word = tidied_word

        # return the longest word found in the chapter
        yield JobOutputEvent({"top_shelf_word": longest_word})


class LongestWordAssembly(Job):
    """Assemble the longest words from each chapter into a unique list."""

    @classmethod
    def run(
        cls, context: Context, input, dependencies
    ) -> Iterator[Job | JobOutputEvent | WorkflowOutputEvent]:
        long_words = set()

        for dep in dependencies.get("chapters"):
            summary = dep.output(context)
            long_words.add(summary["top_shelf_word"])

        yield WorkflowOutputEvent({"the_list": list(long_words)})


scope = LocalScope().add_job_classes([
    BookProcessor,
    ChapterProcessor,
    LongestWordAssembly
])

workflow = Workflow(context=MemoryContext(scope=scope), max_workers=4, stall_time=1)

for event in workflow.run(
    BookProcessor({"file_path": "/home/rg/Code/zahir/integration_tests/data.txt"}, {})
):
    if isinstance(event, WorkflowOutputEvent):
        print(event)
