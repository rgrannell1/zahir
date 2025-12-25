import re
from typing import Iterator
from zahir.context import LocalContext
from zahir.dependencies.job import JobDependency
from zahir.events import JobOutputEvent, WorkflowOutputEvent
from zahir.scope import LocalScope
from zahir.types import Context, Job
from zahir.workflow import Workflow


class BookProcessor(Job):
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

        job_dependencies = {
            "chapters": [JobDependency(pid, context.job_registry) for pid in pids]
        }

        yield LongestWordAssembly({}, job_dependencies)


WORD_RE = re.compile(r"[^\W\d_]+(?:-[^\W\d_]+)*", re.UNICODE)


def longest_word_sequence(text: str) -> str:
    return max(WORD_RE.findall(text), key=len, default="")


class ChapterProcessor(Job):
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

        yield JobOutputEvent({"top_shelf_word": longest_word})


class LongestWordAssembly(Job):
    @classmethod
    def run(
        cls, context: Context, input, dependencies
    ) -> Iterator[Job | JobOutputEvent | WorkflowOutputEvent]:
        long_words = set()

        for dep in dependencies.get("chapters"):
            summary = dep.output(context)
            long_words.add(summary["top_shelf_word"])

        yield WorkflowOutputEvent({"the_list": list(long_words)})


scope = LocalScope()
scope.add_job_classes([BookProcessor, ChapterProcessor, LongestWordAssembly])

workflow = Workflow(context=LocalContext(scope=scope), max_workers=4, stall_time=1)

for event in workflow.run(
    BookProcessor({"file_path": "/home/rg/Code/zahir/integration_tests/data.txt"}, {})
):
    if isinstance(event, WorkflowOutputEvent):
        ...
