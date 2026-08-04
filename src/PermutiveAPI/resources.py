"""Consistent typed resource interfaces built on :class:`PermutiveClient`."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Generic, Iterator, Optional, TypeVar

from .sdk import JSONObject, Page, PermutiveClient

T = TypeVar("T")


@dataclass(frozen=True)
class Resource(Generic[T]):
    """Canonical CRUD/list facade for one API resource family."""

    client: PermutiveClient
    path: str
    decoder: Callable[[JSONObject], T]

    def get(self, resource_id: str) -> T:
        """Return one resource by identifier."""
        return self.decoder(self.client.request("GET", f"{self.path}/{resource_id}"))

    def list_page(
        self, *, page_size: int = 100, continuation: Optional[str] = None
    ) -> Page[T]:
        """Return one typed page."""
        return self.client.list_page(
            self.path,
            item_decoder=self.decoder,
            page_size=page_size,
            continuation=continuation,
        )

    def iter_all(
        self, *, page_size: int = 100, max_items: Optional[int] = None
    ) -> Iterator[T]:
        """Lazily iterate all resources."""
        return self.client.iter_all(
            self.path,
            item_decoder=self.decoder,
            page_size=page_size,
            max_items=max_items,
        )

    def create(self, payload: JSONObject) -> T:
        """Create and return one resource."""
        return self.decoder(self.client.request("POST", self.path, json=payload))

    def update(self, resource_id: str, payload: JSONObject) -> T:
        """Update and return one resource."""
        return self.decoder(
            self.client.request("PATCH", f"{self.path}/{resource_id}", json=payload)
        )

    def delete(self, resource_id: str) -> None:
        """Delete one resource."""
        self.client.request("DELETE", f"{self.path}/{resource_id}")
