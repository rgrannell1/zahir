# 🪙 Zahir

![tests](https://github.com/rgrannell1/zahir/actions/workflows/ci.yml/badge.svg)
[![codecov](https://codecov.io/gh/rgrannell1/zahir/branch/main/graph/badge.svg)](https://codecov.io/gh/rgrannell1/zahir)

> Perhaps I shall conclude by wearing away the Zahir simply through thinking of it again and again.

Zahir is (re)built on the algebraic effects multiprocessing runtime [tertius](https://github.com/rgrannell1/tertius), which itself is based on the algebraic effect library [orbis](https://github.com/rgrannell1/tertius).

It is the simplest workflow engine I could build. It is not a DAG workflow engine or a traditional state-machine workflow engine. It is a dynamically expanding effect-driven state-machine where state transitions are defined at runtime by running jobs. It does not statically define a workflow; the workflow unfolds from the starting step's execution. It looks like normal code, it just happens to run on different processes automatically.

Dependencies, schedules, signals, and jobs themselves use the same generator system: read an input, do computation, wait on other tasks, return a result. Jobs use effects - approximately speaking events that get responses back - to communicate with the workflow engine. This allows for concurrency control or interjob signalling.

## A Simple Workflow

The following example workflow reads the text of a book, splits it into chapters, and `chapter_processor` computes the longest word in each chapter. `longest_word_assembly` depends on these results, aggregates them, and emits an output event with the longest words by chapter.

## How Zahir Works

Zahir builds on [tertius](https://github.com/rgrannell1/tertius), which provides a way of setting up processes and communicating between them. It is a supervisor - worker model.

The overseer process is a GenServer that manages mutable state (the job queue, concurrency limits, etc). Workers too are GenServers, which ask for jobs to run, evaluates them, and enqueues new jobs. They communicate results to the overseer, or awaited job statuses.

## Constructs

### Effects: Request & Respond

Effects are generally abstracted away from jobs in Zahir, but ultimately both the zahir workflow engine & underlying multiprocess runtime are stacked effect systems. For every effect there's a handler; they're handled internally by Zahir, with telemetry simply being another generator composed on top to watch the information flows. The user too can compose on top of the effect system, to facilitate debugging.

**Job-emittable effects**

These effects can be emitted by a job

- `EAwait(fn_name, args, timeout_ms?)`: pause, wait for this job, resume with the result or error on timeout
- `EAwaitAll([ EAwait, ... ])`: pause, wait for N jobs, resume with the results or error if any job failed
- `EAcquire(name, limit)`: acquire a concurrency slot
- `EGetSemaphore(name)`: get a semaphore's state
- `ESetSemaphore(name, state)`: set a semaphore's state
- `ESatisfied(metadata?)`: if a dependency is satisfied, describe why
- `EImpossible(reason)`: if a dependency is impossible, decribe why
- `EEmit(msg)`: emit an event

**Coordination effects**

These are internal effects used by the workflow engine; jobs cannot yield them directly.

*Job lifecycle*
- `EGetJob(worker_pid_bytes)`: request work from the overseer — returns a new job, a buffered child result, or None
- `EEnqueue(fn_name, args, reply_to, timeout_ms, nonce)`: queue a child job and route its result back to this worker
- `EJobComplete(result, reply_to, nonce)`: report successful job completion to the overseer
- `EJobFail(error, reply_to, nonce)`: report job failure (error or timeout) to the overseer
- `ERelease(name)`: release a named concurrency slot back to the overseer

*Overseer queries*
- `EAcquireSlot(name, limit)`: request a named concurrency slot from the overseer
- `ESignal(name)`: query the current state of a named semaphore from the overseer
- `ESetSemaphoreState(name, state)`: write a new state for a named semaphore to the overseer

## Jobs: Do things, wait for things

Jobs do something, based on an input. They can return a value themselves. Most crucially, they can await other jobs by invoking functions registered in scope; for example `res = yield ctx.scope.my_subtask(input)`. Jobs are generators; they can yield out to other jobs (which'll run in parallel) or just call regular functions.

There's no rollbacks; if something goes wrong, detect it with a try-catch or by inspecting returrn values, and run corrective actions yourself. Job-level rollbacks do not compose into workflow rollbacks; crouching, stepping backwards, and taking off your parachute will not get you back on your plane.

Dependencies are jobs that wait for a condition, and yield `ESatisfied` when it's met, sleep with `ESleep` until then, and when the dependency can never be met yields `EImpossible`. Jobs should await suitable conditions to run using dependency-jobs, and post-check with the same mechanism. Zahir ships a few built-in dependency jobs.

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
