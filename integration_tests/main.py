
from typing import Iterator
from zahir.context import LocalContext
from zahir.dependencies.job import JobDependency
from zahir.events import JobOutputEvent, WorkflowOutputEvent
from zahir.scope import LocalScope
from zahir.types import Context, Job, JobOptions
from zahir.workflow import Workflow

class BookProcessor(Job):
  @classmethod
  def run(
      cls,
      context: Context,
      input,
      dependencies) -> Iterator[Job|JobOutputEvent]:

    pids = []
    chapter_lines: list[str] = []

    with open(input["file_path"], "r") as file:
      for line in file:
        if "CHAPTER" in line:
          chapter_job = ChapterProcessor({
            "lines": chapter_lines.copy()
          }, {})
          yield chapter_job
          pids.append(chapter_job.job_id)

          chapter_lines = []

        chapter_lines.append(line)

    job_dependencies = {
      "chapters": [ JobDependency(pid, context.job_registry) for pid in pids ]
    }

    yield LongestWordAssembly({}, job_dependencies)

class ChapterProcessor(Job):
  @classmethod
  def run(
      cls,
      context: Context,
      input,
      dependencies) -> Iterator[Job|JobOutputEvent]:

    longest_word = ""

    for line in input["lines"]:
      for word in line.split():
        if len(word) > len(longest_word):
          longest_word = word

    yield JobOutputEvent({"top_shelf_word": longest_word})


class LongestWordAssembly(Job):
  @classmethod
  def run(
      cls,
      context: Context,
      input,
      dependencies) -> Iterator[Job|JobOutputEvent]:

    long_words = []

    for dep in dependencies.get("chapters"):
      summary = dep.output(context)
      long_words.append(summary["top_shelf_word"])

    yield JobOutputEvent({"long_words": sorted(long_words)})

scope = LocalScope()
scope.add_job_class(BookProcessor)
scope.add_job_class(ChapterProcessor)
scope.add_job_class(LongestWordAssembly)

workflow = Workflow(
  context=LocalContext(scope=scope),
  max_workers=4,
  stall_time=1
)

for event in workflow.run(BookProcessor({
  "file_path": "/home/rg/Code/zahir/integration_tests/data.txt"
}, {})):
  if isinstance(event, WorkflowOutputEvent):
    ...
