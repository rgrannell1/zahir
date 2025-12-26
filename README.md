
# 🪙 Zahir

![tests](https://github.com/rgrannell1/zahir/actions/workflows/ci.yml/badge.svg)

> Perhaps I shall conclude by wearing away the Zahir simply through thinking of it again and again.

## Motivation

I'm writing a self-hosted digital library website, and I need a way of coordinating data-processing. My last workflow engine was written in PowerShell, so I'm not using that...

## File-Structure

```
src/
    dependencies/
        concurrency.py     await a free concurrency slot before starting a job
        time.py            await a particular time-range before starting a job
        group.py           await a group of dependencies
        job.py             await a particular job-state
    event_registry/
        memory.py          register events locally
        sqlite.py          register events in SQLite
    job_registry/
        memory.py          register jobs locally
        sqlite.py          register jobs in SQLite
    context.py             communicates workflow internals with dependencies and jobs
    logging.py             output information about the workflow status
    events.py              events describing workflow state-updates
    exception.py           exceptions thrown by Zahir
    scope.py               handle translation from serialised data to instances
    types.py               abstract types for key Zahir abstractions
    workflow.py            run the workflows
```

## Constructs

![](./zahir.png)

Workflows can be modelled with a few primitives:

### Jobs - Do things & arrange for more things to be done

Jobs do something, based on an input. They can have dependencies that must be met before they run. If they throw an unhandled exception, an optional recovery workflow is scheduled (essentially a "catch" handler).

Workflows comprise jobs that create other jobs. They aren't a separate abstraction; jobs yield further jobs they wish to complete after the current one. This can be done with conditional logic (so conditional workflows are of course supported). No automatic guarantee is given on job execution order (everything that can be run in parallel, is run in parallel). Jobs can however depend on other jobs and their outputs via a `JobDependency`. This allows patterns such as "process each item, await completion & update a database".

Rollbacks are also not separate abstractions; if something goes wrong, detect it and schedule tasks to remediate it. Rollbacks are not job-level as individual job-rollbacks do not necessarily effectively compose into a workflow level rollback.

Data is passed unidirectionally from an initial job to subjobs by the parent job simply yielding the new instantiated job with appropriate input. Jobs may, ultimately, yield a `JobOutputEvent` dictionary. This allows a promise-style call pattern:

- Job A spawns N batch jobs
- Job B awaits this jobs via a list of job-dependencies
- On completion, Job B can access the output data from this array of jobs

This is the most idiomatic way of implementing the "fan-out, then aggregate" pattern in Zahir. In a similar way, workflow-level output can be yielded with `WorkflowOutputEvent`.

Jobs should have most of their logic factored out into plain functions; the job should just take input, call the necessary functions, and delegate to other jobs.

### Dependencies - Await some precondition before doing things

Jobs may have preconditions before running.

- `ConcurrencyLimit`: this dependency is satisfied when the concurrency limit is beneath a cap. Jobs are responsible for acquiring / freeing the concurrency limit when the job starts.
- `JobDependency`: this dependency is satisfied when another job reaches a requested state.
- `TimeDependency`: this dependency is satisfied when the workflow is in a certain time range.
- `GroupDependency`: used to consolidate several dependencies into a single aggregate dependency

Dependencies can be flagged as impossible to fulfill; jobs with impossible dependencies are removed from the `pending` queue and flagged in the event registry.

Dependency implementations must be serialisable to JSON.

### Events - Communicate how the workflow is going

Zahir communicates changes in workflow state as a stream of events emitted by `workflow.run`. These events include metadata. Most are emitted internally by the workflow engine itself:

- `WorkflowCompleteEvent`: workflow complete
- `JobRunnableEvent`
- `JobCompletedEvent`
- `JobStartedEvent`
- `JobOutputEvent`
- `JobTimeoutEvent`
- `JobRecoveryStarted`
- `JobRecoveryCompleted`
- `JobRecoveryTimeout`
- `JobIrrecoverableEvent`
- `JobPrecheckFailedEvent`

A few can be used by jobs to communicate with the workflow engine:

- `WorkflowOutputEvent`: yield output from the workflow. Workflows yield a stream of outputs; since many workflows are long-running it's better to yield results as we go
- `JobOutputEvent`: return output from a job. Treated as a singular return; the task is dropped after this event is yielded.

## Registries - Store workflow state

Workflow orchestrators need to store some operational data.

`JobRegistry` keeps track of which jobs exist, their outputs, and what state they are in.

`EventRegistry` stores the events of a workflow execution.

## Scope - Convert from data to classes

We serialise jobs and dependencies to our registries for storage. We need to translate this data back to the associated Python classes. `Scope` implementations handle this translation. Jobs and Dependencies have to be explicitly registered with a scope for a non-local workflow to run.

## Context - Bundles Zahir internals

We expose internals like the job-registry and scope to dependencies and jobs as a runtime-only value using a `Context` object. This allows us to implement control-flow operations like retries using Jobs directly. This prevents structural lock-in to the control-flow operators shipped with Zahir.

## Logger - Communicate Job State

Workflows run a long time, so we need to communicate how the workflow is proceeding.

## Development

```
rs format
```

```
rs lint
```

```
rs test
```

## License

Copyright (c) 2025 Róisín Grannell

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
