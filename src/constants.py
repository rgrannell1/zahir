DEPENDENCY_DELAY_MS = 5_000
WORKER_POLL_MS = 100
COMPLETION_POLL_MS = 200

# Overseer protocol — messages exchanged between workers and the overseer GenServer
GET_JOB = "get_job"
ENQUEUE = "enqueue"
JOB_DONE = "job_done"
IS_DONE = "is_done"
ACQUIRE = "acquire"
RELEASE = "release"
SIGNAL = "signal"
SET_SEMAPHORE = "set_semaphore"
