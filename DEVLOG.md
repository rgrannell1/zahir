
# Devlog

- Events need to be grouped, we have too many to remember
- Transforms of tasks are not supported
- Curio is similar; they support race and cancellation. We should too. Cancels should propegate. So jobs need parent-ids
- Cancellation should be blockable
- Jobs with tracebacks
- Support return joboutput?
- Ensure that jobs can intercommunicate if they'd like. This is hard since job saving and loading serialises to JSON
- We need a way of communicating progress to the user beyond our janky progress bar
- Implement missing event states
- Actually save events
