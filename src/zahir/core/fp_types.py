"""Functional programming primitives"""

from dataclasses import dataclass
from typing import Generic, TypeVar

ValueT = TypeVar("ValueT")
ErrorT = TypeVar("ErrorT")


@dataclass(frozen=True)
class Ok(Generic[ValueT]):
    """Represents a successful result."""

    value: ValueT


@dataclass(frozen=True)
class Err(Generic[ErrorT]):
    """Represents a failed result."""

    error: ErrorT


type Result[V, E] = Ok[V] | Err[E]
