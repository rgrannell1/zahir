
# Effects

Effects are events with responses, approximately. Layering of job effects from coordination effects insulates jobs from directly altering Zahir runtime internals in-band (the better approach would be modifying handlers)

**coordination.py**

Effects used by the workers to communicate with the overseer process. Not yielded by jobs directly, though some are thin translations of job effects.

**job.py**

Effects a job may yield.

**storage.py**

Storage effects are yielded to the backend to handle stateful storage.
