# 🪙 Zahir

![tests](https://github.com/rgrannell1/zahir/actions/workflows/ci.yml/badge.svg)
[![codecov](https://codecov.io/gh/rgrannell1/zahir/branch/main/graph/badge.svg)](https://codecov.io/gh/rgrannell1/zahir)

> Perhaps I shall conclude by wearing away the Zahir simply through thinking of it again and again.

Zahir is (re)built on the algebraic effects multiprocessing runtime [tertius](https://github.com/rgrannell1/tertius), which itself is based on the algebraic effect library [orbis](https://github.com/rgrannell1/tertius).

It is the simplest workflow engine I could build. It is not a DAG workflow engine or a traditional state-machine workflow engine. It is a dynamically expanding effect-driven state-machine where state transitions are defined at runtime by running jobs. It does not statically define a workflow; the workflow unfolds from the starting step's execution. It looks like normal code, it just happens to run on different processes automatically.

Dependencies, schedules, signals, and jobs themselves use the same generator system: read an input, do computation, wait on other tasks, return a result. Jobs use effects - approximately speaking events that get responses back - to communicate with the workflow engine. This allows for concurrency control or interjob signalling.

## A Simple Workflow

The following example workflow reads the text of a book, splits it into chapters, and `chapter_processor` computes the longest word in each chapter. `longest_word_assembly` fans them out in parallel and returns the results. The root job `book_workflow` returns the final result, which `evaluate` surfaces as the last event in the stream.

```python
from zahir.core.effects import EAwait
from zahir.core.evaluate import JobContext, evaluate


def chapter_processor(ctx: JobContext, chapter: str):
    words = chapter.split()
    return max(words, key=len) if words else ""
    yield  # makes it a generator


def longest_word_assembly(ctx: JobContext, chapters: list[str]):
    results = yield EAwait([
        ctx.scope.chapter_processor(chapter)
        for chapter in chapters
    ])
    return results
    yield


def book_workflow(ctx: JobContext, file_path: str):
    chapters = open(file_path).read().split("\n\n")
    results = yield ctx.scope.longest_word_assembly(chapters)
    return {"longest_words": results}
    yield


scope = {
    "book_workflow": book_workflow,
    "longest_word_assembly": longest_word_assembly,
    "chapter_processor": chapter_processor,
}

for event in evaluate("book_workflow", ("war-and-peace.txt",), scope, n_workers=4):
    print(event)  # {"longest_words": [...]}
```

## How Zahir Works

Zahir builds on [tertius](https://github.com/rgrannell1/tertius), which provides a way of setting up processes and communicating between them. It is a supervisor - worker model.

The overseer process is a GenServer that manages mutable state (the job queue, concurrency limits, etc). Workers too are GenServers, which ask for jobs to run, evaluates them, and enqueues new jobs. They communicate results to the overseer, or awaited job statuses.

## Constructs

### Effects: Request & Respond

Effects are generally abstracted away from jobs in Zahir, but ultimately both the zahir workflow engine & underlying multiprocess runtime are stacked effect systems. For every effect there's a handler; they're handled internally by Zahir, with telemetry simply being another generator composed on top to watch the information flows. The user too can compose on top of the effect system, to facilitate debugging.

**Job-emittable effects**

These effects can be yielded by a job.

- `yield EAwait(job | job[])`: pause, wait for one or more jobs in parallel, resume with the result or a list of results in dispatch order. Raises `JobError` if any job failed. `yield ctx.scope.myjob()` is shorthand for the single-job form.
- `yield EAcquire(name, limit)`: acquire a named concurrency slot
- `yield EGetSemaphore(name)`: get a semaphore's state
- `yield ESetSemaphore(name, state)`: set a semaphore's state
- `yield EEmit(msg)`: emit an event to the `evaluate` caller

**Coordination effects**

These are internal effects used by the workflow engine; jobs cannot yield them directly.

*Job lifecycle*
- `EGetJob(worker_pid_bytes)`: request work from the overseer — returns a new job, a buffered child result, or None
- `EEnqueue(fn_name, args, reply_to, timeout_ms, sequence_number)`: queue a child job and route its result back to this worker
- `EJobComplete(result, reply_to, sequence_number)`: report successful job completion to the overseer
- `EJobFail(error, reply_to, sequence_number)`: report job failure (error or timeout) to the overseer
- `ERelease(name)`: release a named concurrency slot back to the overseer

*Overseer queries*
- `EAcquireSlot(name, limit)`: request a named concurrency slot from the overseer
- `ESignal(name)`: query the current state of a named semaphore from the overseer
- `ESetSemaphoreState(name, state)`: write a new state for a named semaphore to the overseer
- `EIsDone()`: ask the overseer whether all pending jobs have completed
- `EGetError()`: retrieve the root error from the overseer, if any
- `EGetResult()`: retrieve the root job's return value from the overseer

## Jobs: Do things, wait for things

Jobs do something, based on an input, and return a value. Jobs can await other jobs by invoking functions registered in scope: `res = yield ctx.scope.my_subtask(input)`. Jobs are generators; they can fan out to other jobs (which run in parallel) or just call regular functions.

There's no rollbacks; if something goes wrong, detect it with a try-catch or by inspecting returrn values, and run corrective actions yourself. Job-level rollbacks do not compose into workflow rollbacks; crouching, stepping backwards, and taking off your parachute will not get you back on your plane.

Dependencies are jobs that wait for a condition, and yield `ESatisfied` when it's met, sleep with `ESleep` until then, and when the dependency can never be met yields `EImpossible`. We can construct them with a combinator function that takes a function yielding those dependency results; it handles sleeping until a result is returned.

Jobs should await suitable conditions to run using dependency-jobs, and post-check with the same mechanism. Zahir ships a few built-in dependency jobs.

- `concurrency_dependency(name, limit, timeout_ms?)`: wait until a slot is free to run using an underlying `EAcquire` semaphore poll.
- `semaphore_dependency(name, timeout_ms?)`: wait until a named semaphore signals satisfied or impossible. Another job can do this using `ESetSemaphore`, to coordinate starts or stops.
- `resource_dependency(resource, max_percent, timeout?)`: wait until memory or CPU usage is acceptable low.
- `sqlite_dependency(db_path, query, params?, timeout_seconds?)`: wait until a query returns rows (or alternatively, a1x1 query says `satisfied` or `impossible`)
- `time_dependency(before?, after?)`: wait until a time window; return `impossible` if it's passed
- `group_dependency([...])`: sequence a list of dependencies

## Context: Job-accessible internals

We need to pre-agree on what names map to what functions; scope is a dictionary of names to functions. Jobs can be called using `ctx.scope.name(...args)`; of course, normal functions are accessible in the usual way.

## License

Copyright © 2026 Róisín Grannell

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
