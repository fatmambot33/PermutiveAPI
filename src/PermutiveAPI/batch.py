"""Typed batch execution result models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Optional, Sequence, TypeVar

T = TypeVar("T")
I = TypeVar("I")


@dataclass(frozen=True)
class BatchItemResult(Generic[I, T]):
    """Represent the result for one input item."""

    item: I
    value: Optional[T] = None
    error: Optional[Exception] = None

    @property
    def succeeded(self) -> bool:
        """Return whether the item completed successfully."""
        return self.error is None


@dataclass(frozen=True)
class BatchResult(Generic[I, T]):
    """Represent a complete ordered batch result."""

    items: Sequence[BatchItemResult[I, T]]

    @property
    def successes(self) -> Sequence[BatchItemResult[I, T]]:
        """Return successful item results."""
        return tuple(item for item in self.items if item.succeeded)

    @property
    def failures(self) -> Sequence[BatchItemResult[I, T]]:
        """Return failed item results."""
        return tuple(item for item in self.items if not item.succeeded)


__all__ = ["BatchItemResult", "BatchResult"]
