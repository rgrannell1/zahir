DEPENDENCY_DELAY_MS = 5_000
WORKER_POLL_MS = 100
COMPLETION_POLL_MS = 200

# Semaphore and dependency status values — the three states a dependency or semaphore can be in
SATISFIED = "satisfied"
UNSATISFIED = "unsatisfied"
IMPOSSIBLE = "impossible"

# How long to wait between CPU usage samples when checking resource availability
CPU_SAMPLE_INTERVAL_S = 0.1

# Overseer protocol — messages exchanged between workers and the overseer GenServer
GET_JOB = "get_job"
ENQUEUE = "enqueue"
JOB_DONE = "job_done"
IS_DONE = "is_done"
GET_ERROR = "get_error"
ACQUIRE = "acquire"
RELEASE = "release"
SIGNAL = "signal"
SET_SEMAPHORE = "set_semaphore"
