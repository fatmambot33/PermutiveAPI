"""Tests for asynchronous resource, pagination, and batch operations."""

from __future__ import annotations

from typing import Any, Mapping

import pytest

from PermutiveAPI.async_operations import AsyncResource, execute_async_batch


class StubClient:
    """Small asynchronous client double."""

    def __init__(self, responses: list[dict[str, Any]]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    async def request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        """Record and return the next payload."""
        self.calls.append((method, path, kwargs))
        return self.responses.pop(0)


@pytest.mark.asyncio
async def test_async_resource_lists_and_iterates_pages() -> None:
    """Ensure pagination preserves ordering and continuation semantics."""
    client = StubClient(
        [
            {"items": [{"id": "1"}], "continuation": "next"},
            {"items": [{"id": "2"}]},
        ]
    )
    resource = AsyncResource(client, "cohorts", lambda item: item["id"])  # type: ignore[arg-type]

    items = [item async for item in resource.iter_all()]

    assert items == ["1", "2"]
    assert client.calls[1][2]["params"]["continuation"] == "next"


@pytest.mark.asyncio
async def test_async_batch_is_bounded_ordered_and_collects_errors() -> None:
    """Ensure batch outcomes retain input order and typed errors."""
    active = 0
    maximum = 0

    async def operation(value: int) -> int:
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        try:
            if value == 2:
                raise ValueError("bad item")
            return value * 10
        finally:
            active -= 1

    results = await execute_async_batch([1, 2, 3], operation, concurrency=2)

    assert maximum <= 2
    assert [result.item for result in results] == [1, 2, 3]
    assert results[0].value == 10
    assert isinstance(results[1].error, ValueError)
    assert results[2].value == 30


@pytest.mark.asyncio
async def test_async_batch_fail_fast_propagates() -> None:
    """Ensure fail-fast mode propagates the first operation failure."""

    async def operation(value: int) -> int:
        if value == 2:
            raise RuntimeError("stop")
        return value

    with pytest.raises(RuntimeError, match="stop"):
        await execute_async_batch([1, 2, 3], operation, fail_fast=True)
