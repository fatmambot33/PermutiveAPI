"""Tests for asynchronous resource, pagination, and batch parity."""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Mapping, Optional, Tuple

import pytest

from PermutiveAPI import (
    AsyncPermutiveClient,
    AsyncResource,
    DecodingError,
    execute_async_batch,
)


class FakeResponse:
    """Small asynchronous response double."""

    def __init__(
        self,
        payload: Any,
        *,
        status_code: int = 200,
        headers: Optional[Mapping[str, str]] = None,
    ) -> None:
        self.status_code = status_code
        self._payload = payload
        self.content = b"payload"
        self.headers = dict(headers or {})

    def json(self) -> Any:
        """Return the configured payload."""
        return self._payload


class FakeTransport:
    """Ordered asynchronous transport double."""

    def __init__(self, *responses: FakeResponse) -> None:
        self.responses = list(responses)
        self.requests: List[Tuple[str, str, Dict[str, Any]]] = []
        self.closed = False

    async def request(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
        """Record and return the next response."""
        self.requests.append((method, url, kwargs))
        return self.responses.pop(0)

    async def aclose(self) -> None:
        """Record transport closure."""
        self.closed = True


def decode_id(payload: Dict[str, Any]) -> str:
    """Decode one identifier from a test payload."""
    return str(payload["id"])


@pytest.mark.asyncio
async def test_async_pagination_matches_sync_semantics() -> None:
    """Fetch pages lazily while preserving order and maximum-item bounds."""
    transport = FakeTransport(
        FakeResponse({"items": [{"id": "a"}, {"id": "b"}], "continuation": "n"}),
        FakeResponse({"items": [{"id": "c"}]}),
    )
    client = AsyncPermutiveClient("secret", transport=transport)

    values = [
        value
        async for value in client.iter_all(
            "cohorts", item_decoder=decode_id, page_size=2, max_items=3
        )
    ]

    assert values == ["a", "b", "c"]
    assert transport.requests[0][2]["params"] == {"limit": 2, "k": "secret"}
    assert transport.requests[1][2]["params"] == {
        "limit": 2,
        "continuation": "n",
        "k": "secret",
    }


@pytest.mark.asyncio
async def test_async_pagination_rejects_repeated_tokens() -> None:
    """Stop malformed pagination loops deterministically."""
    transport = FakeTransport(
        FakeResponse({"items": [], "continuation": "same"}),
        FakeResponse({"items": [], "continuation": "same"}),
    )
    client = AsyncPermutiveClient("secret", transport=transport)

    with pytest.raises(DecodingError, match="Repeated pagination"):
        async for _ in client.iter_all("cohorts", item_decoder=decode_id):
            pass


@pytest.mark.asyncio
async def test_async_resource_delegates_crud_to_client() -> None:
    """Keep resource operations thin and ordered over the canonical client."""
    transport = FakeTransport(
        FakeResponse({"id": "one"}),
        FakeResponse({"id": "two"}),
        FakeResponse({"id": "three"}),
        FakeResponse({}, status_code=204),
    )
    resource = AsyncResource(
        AsyncPermutiveClient("secret", transport=transport), "cohorts", decode_id
    )

    assert await resource.get("one") == "one"
    assert await resource.create({"name": "two"}) == "two"
    assert await resource.update("two", {"name": "three"}) == "three"
    assert await resource.delete("three") is None
    assert [request[0] for request in transport.requests] == [
        "GET",
        "POST",
        "PATCH",
        "DELETE",
    ]


@pytest.mark.asyncio
async def test_async_batch_is_bounded_ordered_and_typed() -> None:
    """Return ordered outcomes despite out-of-order completion and failures."""
    active = 0
    peak = 0

    async def operation(value: int) -> int:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.sleep(0.001 * (4 - value))
            if value == 2:
                raise ValueError("boom")
            return value * 10
        finally:
            active -= 1

    result = await execute_async_batch([1, 2, 3], operation, max_concurrency=2)

    assert peak == 2
    assert [item.input for item in result.items] == [1, 2, 3]
    assert [item.value for item in result.successes] == [10, 30]
    assert isinstance(result.failures[0].error, ValueError)


@pytest.mark.asyncio
async def test_async_batch_cancels_children_when_parent_is_cancelled() -> None:
    """Collect child cancellation before propagating parent cancellation."""
    cancelled = asyncio.Event()

    async def operation(_: int) -> int:
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    task = asyncio.create_task(execute_async_batch([1, 2], operation))
    await asyncio.sleep(0)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert cancelled.is_set()
