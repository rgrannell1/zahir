
# Dependencies

Zahir runs jobs, which over the course of time read input, do things, and produce output. Dependencies and their constituent parts are a special variety of job. Dependencies have an underlying three-state condition function (satisfied, unsatisfied, impossible) that is evaluated once. We construct dependencu jobs using a combinator that polls while unsatisfied, terminates on satisfied or impossible.

In practice, dependencies block execution until a world-state is true; a time is reached, a resource exists or does not exist, we have enough RAM to avoid crashing our laptop (again).
