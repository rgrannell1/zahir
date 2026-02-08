"""Transform type alias and TransformSpec dataclass.

A Transform is a higher-order function that wraps a JobSpec to
produce a modified JobSpec — e.g. adding retry behaviour or logging.

TransformSpec pairs a transform type-label with arguments, and knows
how to serialise/deserialise itself.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from zahir.types.serialised import SerialisedTransformSpec

if TYPE_CHECKING:
    from zahir.types.job import JobSpec

# Transforms take a spec and give a new one.
# The args are a dictionary of transform-specific arguments.
type Transform = Callable[[Mapping[str, Any], "JobSpec"], "JobSpec"]


@dataclass
class TransformSpec:
    """A specification for a transform to apply to a JobSpec.

    Transforms are looked up by type in the scope, then applied with the given args
    to produce a transformed JobSpec.
    """

    # The type label for this transform (used for scope lookup)
    type: str

    # Arguments to pass to the transform function
    args: Mapping[str, Any] = field(default_factory=dict)

    def save(self) -> SerialisedTransformSpec:
        """Serialise the transform spec to a dictionary."""
        return {
            "type": self.type,
            "args": dict(self.args),
        }

    @classmethod
    def load(cls, data: SerialisedTransformSpec) -> "TransformSpec":
        """Load a transform spec from serialised data."""
        return cls(type=data["type"], args=data.get("args", {}))
