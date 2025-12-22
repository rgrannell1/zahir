
# Zahir

## Motivation

I'm writing a self-hosted digital library website, and I need a way of coordinating data-processing. My last workflow engine was written in PowerShell, so I'm not using that...

## File-Structure

```
src/
    dependencies/
        concurrency.py
```

## Constructs

Workflows can be modelled with a few primitives

### Tasks

We stitch tasks together into a workflow. Tasks typically await tasks before beginning. Tasks may request other tasks start (though these two may depend on their own tasks).

Tasks define:
- A namespaced state-label, denoting what type of task this is
- Pre-conditions and post-conditions they expect to be true (optional)
- Signals required for the task to start (optional)
- Downstream tasks this task might invoke. This is requires, so we can statically check
    that the types line up in advance.

### Limits

We need to coordinate simited central-state across a workflow; things like locking, rate-limits.

## License

Copyright (c) 2025 Róisín Grannell

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.