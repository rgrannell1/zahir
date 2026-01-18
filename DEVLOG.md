
# Devlog

- Events need to be grouped, we have too many to remember
- Transforms of tasks are not supported, yet!
- Curio is similar; they support race and cancellation. We should too. Cancels should propegate.
- Cancellation should be blockable
- Jobs with tracebacks
- We need a way of communicating progress to the user beyond our janky progress bar



## Progress Redesign

Zahir |  | CPU <avg CPU 5s>% RAM <avg RAM 5s>%

JobType [ status broken by percentage ] (terminal) / (total
)