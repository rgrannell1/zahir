# 🪙 Zahir

![tests](https://github.com/rgrannell1/zahir/actions/workflows/ci.yml/badge.svg)
[![codecov](https://codecov.io/gh/rgrannell1/zahir/branch/main/graph/badge.svg)](https://codecov.io/gh/rgrannell1/zahir)

> Perhaps I shall conclude by wearing away the Zahir simply through thinking of it again and again.

Zahir is (re)built on the algebraic effects multiprocessing runtime [tertius](https://github.com/rgrannell1/tertius), which itself is based on the algebraic effect library [orbis](https://github.com/rgrannell1/orbis).

It is the simplest workflow engine I could build. It runs a job (a python generator) that can yield further jobs to run sequentially or in parallel. These jobs are distributed across processes automatically. We schedule by waiting for the time to arrive. We avoid memory exhaustion by waiting for it to be free, if we choose. We maintain idempotency with if-then checks. We roll back with try-catch. We pass state with input parameters and returns.

## Architecture

Zahir composes a few single-purpose libraries together to construct its workflow engine.

**Orbis**

Defines the algebraic effect system; requests yielded from generators, intercepted by handlers (which might yield more effects), and responded to. Separates out what we do from how we do it, and helps split concerns like logging and core logic.

**Tertius**

The concurrency layer, built on Orbis. Uses ZMQ for interprocess communication. Defines effects for interprocess messaging, spawning, and telemetry. Defines an Erlang-like genserver interface for using these intercommunicating processes.

**Zahir**

Defines a workflow runtime on top of Tertius's concurrent-process model. Defines effects for modelling workflows like `EAwait`. We spawn workers that poll for jobs, advance them, and communicate results to the overseer which delegates to the storage layer. In practice; we define jobs (fairly straightforward python generators), and zahir spreads the requested workload out across multiple processes.

**Bookman**

Telemetry is composed onto the effect handler layer; the events emited are envelopes for bookman events that capture data about what executed when, for how long. Metric aggregation (e.g in progress bars) is implemented using bookman.

## Modules

- [Effects](src/zahir/core/effects/README.md) - our event-response signals
- [Evaluate](src/zahir/core/evaluate/README.md) - the bit that runs
- [Dependencies](src/zahir/core/dependencies/README.md) - built-in world-state checks & pollers
- [Backends](src/zahir/core/backends/README.md) - swappable storage backend implementations
- [Metrics](src/zahir/progress_bar/metrics/README.md) - progress bar aggregators built on bookman

## A Simple Workflow

The following example workflow reads the text of a book, splits it into chapters, and `chapter_processor` computes the longest word in each chapter. `longest_word_assembly` fans them out in parallel and returns the results. The root job `book_workflow` returns the final result, which `evaluate` surfaces as the last event in the stream.

```python
from zahir.core.effects import await_all
from zahir.core.evaluate import JobContext, evaluate


def chapter_processor(ctx: JobContext, chapter: str):
    words = chapter.split()
    return max(words, key=len) if words else ""
    yield  # makes it a generator


def longest_word_assembly(ctx: JobContext, chapters: list[str]):
    results = yield await_all([
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

## Constructs

## Jobs: Do things, wait for things

Jobs do something, based on an input, and return a value. Jobs can await other jobs by invoking functions registered in scope: `res = yield ctx.scope.my_subtask(input)`. Jobs are generators; they can fan out to other jobs (which run in parallel) or just call regular functions.

There's no rollbacks; if something goes wrong, detect it with a try-catch or by inspecting returrn values, and run corrective actions yourself.

Dependencies are jobs that wait for a condition. They return a `DependencyResult` tuple — `(DependencyState.SATISFIED, metadata)` when met, `(DependencyState.IMPOSSIBLE, reason)` when the condition can never be met. A combinator function handles the polling and sleeping until a result is returned.

Jobs should await suitable conditions to run using dependency-jobs, and post-check with the same mechanism. Zahir ships a few built-in dependency jobs.

- `concurrency_dependency(name, limit, timeout_ms?)`: wait until a slot is free to run using an underlying `EAcquire` semaphore poll.
- `semaphore_dependency(name, timeout_ms?)`: wait until a named semaphore signals satisfied or impossible. Another job can do this using `ESetSemaphore`, to coordinate starts or stops.
- `resource_dependency(resource, max_percent, timeout?)`: wait until memory or CPU usage is acceptable low.
- `sqlite_dependency(db_path, query, params?, timeout_seconds?)`: wait until a query returns rows (or alternatively, a1x1 query says `satisfied` or `impossible`)
- `time_dependency(before?, after?)`: wait until a time window; return `impossible` if it's passed
- `file_dependency(fpath)`: wait until a file exists
- `group_dependency([...])`: sequence a list of dependencies

### Effects: Request & Respond

Effects are generally abstracted away from jobs in Zahir, but ultimately both the zahir workflow engine & underlying multiprocess runtime are stacked effect systems. For every effect there's a handler; they're handled internally by Zahir, with telemetry simply being another generator composed on top to watch the information flows. The user too can compose on top of the effect system, to facilitate debugging. Most are internal; these can be yielded by a job

- `yield ctx.scope.myjob(args)`: dispatch a single job and resume with its result. Raises `JobError` if the job failed.
- `yield await_all([ctx.scope.myjob(args), ...])`: dispatch multiple jobs in parallel, resume with results as a list in dispatch order. Raises `JobError` if any job failed.
- `yield EGetSemaphore(name)`: get a semaphore's state
- `yield ESetSemaphore(name, state)`: set a semaphore's state
- `yield EEmit(msg)`: emit an event to the `evaluate` caller (from `tertius`)

## Context: Job-accessible internals

A detail; we need to pre-agree on what names map to what functions across processes; scope is a dictionary of names to functions. Jobs can be called using `ctx.scope.name(...args)`; of course, normal functions are accessible in the usual way.

## Modelling Workflows

### Scheduling & Idempotency

Scheduling is conceptually waiting for `if wordstate: do thing`. This includes waiting for a time window, for a signal from another process, or for a resource to enter a certain state. Idempotency is similarily a check on worldstates before proceeding, so could be modelled with `sqlite_condition` if we want to externalise state storage.

```python
def process_image(ctx: JobContext, path: str):
    yield from concurrency_dependency("image_workers", limit=4)
    return encode_image(path)
    yield
```


## Checkpointing & Retries

Retries can be modelled at the job-level. Rollbacks are simply `try-catch` usage; if one code-path fails, proceed along another. For checkpointing, externalise state in persistent storage. I have another library, [Funes](https://github.com/rgrannell1/funes) which cleanly supports caching of expensive computations which fits naturally with Zahir. I think ultimately durable jobs are the wrong abstraction; durable computation results achieves much of the benefits at far lower architectural cost.

```python
def fetch_with_retry(ctx: JobContext, url: str):
    for attempt in range(3):
        try:
            result = yield ctx.scope.fetch(url)
            return result
        except JobError:
            if attempt == 2:
                raise
    yield
```

## State Passing

Pass parameters between jobs, or externalise state into SQLite or a similar store

## License

Copyright © 2026 Róisín Grannell

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
