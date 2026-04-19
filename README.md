# 🪙 Zahir

![tests](https://github.com/rgrannell1/zahir/actions/workflows/ci.yml/badge.svg)
[![codecov](https://codecov.io/gh/rgrannell1/zahir/branch/main/graph/badge.svg)](https://codecov.io/gh/rgrannell1/zahir)

> Perhaps I shall conclude by wearing away the Zahir simply through thinking of it again and again.

Zahir is (re)built on the slgebraic effects multiprocessing runtime [tertius](https://github.com/rgrannell1/tertius), which itself is based on the algebraic effect library [orbis](https://github.com/rgrannell1/tertius).

It is the simplest workflow engine I could build. It is not a DAG workflow engine or a traditional state-machine workflow engine. It is a dynamically expanding event-driven state-machine where state transitions are defined at runtime by running jobs. It does not statically define a workflow; the workflow unfolds from the starting step's execution.

Dependencies, schedules, signals, and jobs use the same generator system: read an input, do computation, wait on other tasks, return a result. Jobs use effects - approximately speaking events that get responses back - to communicate with the workflow engine. This allows for concurrency control or interjob signalling.

## A Simple Workflow

The following example workflow reads the text of a book, splits it into chapters, and `chapter_processor` computes the longest word in each chapter. `longest_word_assembly` depends on these results, aggregates them, and emits an output event with the longest words by chapter.

## Constructs

### Effects: Request & Respond

Effects are generally abstracted away from jobs in Zahir, but ultimately both the zahir workflow engine & underlying multiprocess runtime are stacked effect systems.

**Job-emittable effects**

These effects can be emitted by a job

- `EAwait(fn_name, args, timeout_ms?)`: pause, wait for this job, resume with the result or error on timeout
- `EAwaitAll([ EAwait, ... ])`: pause, wait for N jobs, resume with the results or error if any job failed
- `EAcquire(name, limit)`: acquire a concurrency slot
- `EGetSemaphore(name)`: get a semaphore's state
- `ESetSemaphore(name, state)`: set a semaphore's state
- `ESatisfied(metadata?)`: if a dependency is satisfied, describe why
- `EImpossible(reason)`: if a dependency is impossible, decribe why

**Coordination effects**

These are internal effects used as part of the workflow engine.

- `EEnqueue(fn_name, args, timeout_ms, nonce)`: queue a job & where to route the results
- `ERelease(name)`: Release a concurrency hold
- `EGetJob()`:
- `EJobComplete(result, reply_to, nonce)`: a job completed successfully
- `EJobFailed(result, reply_to, nonce)`: a job failed

## License

Copyright © 2026 Róisín Grannell

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.