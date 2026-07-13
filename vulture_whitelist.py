"""Vulture whitelist: names read dynamically, not by direct attribute access.

TypedDict handler-map fields are looked up by effect tag at runtime;
render() is called by rich; __signature__ is read by inspect.
"""

_.__signature__  # noqa: F821 # scope_proxy dispatch introspection
_.storage_get_job  # noqa: F821
_.storage_enqueue  # noqa: F821
_.storage_job_done  # noqa: F821
_.storage_job_failed  # noqa: F821
_.storage_acquire  # noqa: F821
_.storage_release  # noqa: F821
_.storage_signal  # noqa: F821
_.storage_set_semaphore  # noqa: F821
_.storage_is_done  # noqa: F821
_.storage_get_error  # noqa: F821
_.storage_get_result  # noqa: F821
_.get_semaphore  # noqa: F821
_.acquire_slot  # noqa: F821
_.job_complete  # noqa: F821
_.job_fail  # noqa: F821
_.set_semaphore_state  # noqa: F821
_.render  # noqa: F821 # rich ProgressColumn override
