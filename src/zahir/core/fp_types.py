"""Functional programming primitives"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Ok[ValueT]:
    """Represents a successful result."""

    value: ValueT


@dataclass(frozen=True)
class Err[ErrorT]:
    """Represents a failed result."""

    error: ErrorT


type Result[V, E] = Ok[V] | Err[E]
