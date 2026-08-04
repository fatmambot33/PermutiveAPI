"""Typed resource, pagination, and batch operations for the async client."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import (
    AsyncIterator,
    Awaitable,
    Callable,
    Generic,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    cast,
)

from .async_client import AsyncPermutiveClient
from .sdk import DecodingError, JSONObject, JSONScalar, Page

T = TypeVar("T")
I = TypeVar("I")
R = TypeVar("R")


class AsyncResource(Generic[T]):
    """Canonical asynchronous CRUD and pagination facade."""

    def __init__(
        self,
        client: AsyncPermutiveClient,
        path: str,
        decoder: Callable[[JSONObject], T],
    ) -> None:
        self.client = client
        self.path = path
        self.decoder = decoder

    async def get(self, resource_id: str) -> T:
        """Return one resource by identifier."""
        return self.decoder(await self.client.request("GET", f"{self.path}/{resource_id}"))

    async def list(
        self, *, page_size: int = 100, continuation: Optional[str] = None
    ) -> Page[T]:
        """Return one typed page."""
        if page_size < 1:
            raise ValueError("page_size must be positive")
        params: dict[str, JSONScalar] = {"limit": page_size}
        if continuation:
            params["continuation"] = continuation
        payload = await self.client.request("GET", self.path, params=params)
        raw_items = payload.get("items", [])
        if not isinstance(raw_items, list):
            raise DecodingError("Paginated response field 'items' must be a list")
        items = tuple(
            self.decoder(cast(JSONObject, item))
            for item in raw_items
            if isinstance(item, dict)
        )
        token = payload.get("continuation") or payload.get("next_token")
        return Page(items=items, next_token=token if isinstance(token, str) else None)

    async def list_page(
        self, *, page_size: int = 100, continuation: Optional[str] = None
    ) -> Page[T]:
        """Return one page using the compatibility method name."""
        return await self.list(page_size=page_size, continuation=continuation)

    async def iter_all(
        self, *, page_size: int = 100, max_items: Optional[int] = None
    ) -> AsyncIterator[T]:
        """Iterate lazily with repeated-token and item-limit guards."""
        token: Optional[str] = None
        seen: set[str] = set()
        yielded = 0
        while True:
            page = await self.list(page_size=page_size, continuation=token)
            for item in page.items:
                if max_items is not None and yielded >= max_items:
                    return
                yielded += 1
                yield item
            token = page.next_token
            if token is None:
                return
            if token in seen:
                raise DecodingError("Repeated pagination continuation token")
            seen.add(token)

    async def create(self, payload: JSONObject) -> T:
        """Create and return one resource."""
        return self.decoder(await self.client.request("POST", self.path, json=payload))

    async def update(self, resource_id: str, payload: JSONObject) -> T:
        """Update and return one resource."""
        result = await self.client.request(
            "PATCH", f"{self.path}/{resource_id}", json=payload
        )
        return self.decoder(result)

    async def delete(self, resource_id: str) -> None:
        """Delete one resource."""
        await self.client.request("DELETE", f"{self.path}/{resource_id}")


@dataclass(frozen=True)
class AsyncBatchItem(Generic[I, R]):
    """Outcome for one asynchronous batch item."""

    index: int
    item: I
    value: Optional[R] = None
    error: Optional[BaseException] = None

    @property
    def succeeded(self) -> bool:
        """Return whether the item completed successfully."""
        return self.error is None


async def execute_async_batch(
    items: Iterable[I],
    operation: Callable[[I], Awaitable[R]],
    *,
    concurrency: int = 4,
    fail_fast: bool = False,
) -> Tuple[AsyncBatchItem[I, R], ...]:
    """Execute asynchronous work with bounded concurrency and stable ordering."""
    if concurrency < 1:
        raise ValueError("concurrency must be positive")
    values: Sequence[I] = tuple(items)
    semaphore = asyncio.Semaphore(concurrency)

    async def run(index: int, item: I) -> AsyncBatchItem[I, R]:
        async with semaphore:
            try:
                return AsyncBatchItem(index=index, item=item, value=await operation(item))
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                if fail_fast:
                    raise
                return AsyncBatchItem(index=index, item=item, error=exc)

    tasks: List[asyncio.Task[AsyncBatchItem[I, R]]] = [
        asyncio.create_task(run(index, item)) for index, item in enumerate(values)
    ]
    try:
        results = await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    return tuple(sorted(results, key=lambda result: result.index))
