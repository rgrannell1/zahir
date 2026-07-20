"""Vulture whitelist: names read dynamically, not by direct attribute access.

render() is called by rich; __signature__ is read by inspect.
"""

_.__signature__  # noqa: F821 # scope_proxy dispatch introspection
_.render  # noqa: F821 # rich ProgressColumn override
