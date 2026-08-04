"""Reusable typed pagination helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Generic, Iterator, Optional, Sequence, Set, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class Page(Generic[T]):
    """Represent one immutable page of API results."""

    items: Sequence[T]
    next_token: Optional[str] = None


def iter_pages(
    fetch_page: Callable[[Optional[str]], Page[T]],
    *,
    max_items: Optional[int] = None,
) -> Iterator[T]:
    """Yield items lazily while protecting against repeated tokens."""
    token: Optional[str] = None
    seen: Set[str] = set()
    emitted = 0

    while True:
        page = fetch_page(token)
        for item in page.items:
            if max_items is not None and emitted >= max_items:
                return
            yield item
            emitted += 1

        token = page.next_token
        if token is None:
            return
        if token in seen:
            raise RuntimeError("Pagination token repeated; refusing an infinite loop.")
        seen.add(token)


__all__ = ["Page", "iter_pages"]
