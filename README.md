
# Zahir

![tests](https://github.com/rgrannell1/zahir/actions/workflows/ci.yml/badge.svg)

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
    registries/
        local.py           register jobs & events locally
        sqlite.py          register jobs & events in SQLite
    context.py             communicates workflow internals with dependencies and jobs
    events.py              events describing workflow state-updates
    exception.py           exceptions thrown by Zahir
    scope.py               handle translation from serialised data to instances
    types.py               abstract types for key Zahir abstractions
    workflow.py            run the workflows
```

## Constructs

![](./zahir.png)

Workflows can be modelled with a few primitives:

### Jobs

Jobs do something, based on an input. They can have dependencies that must be met before they run. If they throw an unhandled exception, an optional recovery workflow is scheduled. If a more general rollback pattern is desired, detect the failure condition in some job and schedule a tidyup job.

Jobs may yield other jobs they wish to complete after the current one. This can be done with conditional logic (so conditional workflows are of course supported). No automatic guarantee is given on job execution order (everything that can be run in parallel, is run in parallel), but yielding to a `Job` that depends on other jobs will preserve ordering. This allows patterns such as "process each item, await completion & update a database".

Data is passed unidirectionally from an initial job to subjobs, by the parent job simply yielding the new instantiated job. Jobs may, ultimately, yield an output dictionary. This allows a promise-style call pattern:

- Job A spawns N batch jobs
- Job B awaits this jobs via a list of job-dependencies
- On completion, Job b can access the output data from this array of jobs

This is the most idiomatic way of implementing the "fan-out, then aggregate" pattern in Zahir.

### Dependencies

Jobs may have preconditions before running.

- `ConcurrencyLimit`: this dependency is satisfied when the concurrency limit is beneath a cap. Jobs are responsible for acquiring / freeing the concurrency limit when the job starts.
- `JobDependency`: this dependency is satisfied when another job reaches a requested state.
- `TimeDependency`: this dependency is satisfied when the workflow is in a certain time range.
- `GroupDependency`: used to consolidate several dependencies into a single aggregate dependency

Dependencies can be flagged as impossible to fulfill; jobs with impossible dependencies are removed from the `pending` queue and flagged in the event registry.

Dependency implementations must be serialisable to JSON.

### Events

Zahir communicates changes in workflow state as a stream of events emitted by `workflow.run`. These events include metadata, the list is currently:

- `WorkflowCompleteEvent`: workflow complete
- `JobRunnableEvent`
- `JobCompletedEvent`
- `JobStartedEvent`
- `JobTimeoutEvent`
- `JobRecoveryStarted`
- `JobRecoveryCompleted`
- `JobRecoveryTimeout`
- `JobIrrecoverableEvent`
- `JobPrecheckFailedEvent`

## Registries

Workflow orchestrators need to store some operational data.

`JobRegistry` keeps track of which jobs exist, their outputs, and what state they are in.

`EventRegistry` stores the events of a workflow execution.

## Scope

We serialise jobs and dependencies to our registries for storage. We need to translate this data back to the associated Python classes. `Scope` implementations handle this translation. Jobs and Dependencies have to be explicitly registered with a scope for a non-local workflow to run.

## Context

We expose internals like the job-registry and scope to dependencies and jobs as a runtime-only value using a `Context` object. This allows us to implement control-flow operations like retries using Jobs directly. This prevents structural lock-in to the control-flow operators shipped with Zahir.

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
