"""Tests for the typed asynchronous client."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Tuple

import pytest

from PermutiveAPI import AsyncPermutiveClient, AuthenticationError, RetryPolicy


class FakeResponse:
    """Small response double for asynchronous transport tests."""

    def __init__(
        self,
        status_code: int,
        payload: Any,
        *,
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


@pytest.mark.asyncio
async def test_async_request_uses_typed_transport_and_closes() -> None:
    """Exercise success, timeout, and context management."""
    transport = FakeTransport(FakeResponse(200, {"id": "cohort-1"}))

    async with AsyncPermutiveClient("secret", transport=transport) as client:
        payload = await client.request("GET", "/cohorts")

    assert payload == {"id": "cohort-1"}
    assert transport.closed
    method, url, kwargs = transport.requests[0]
    assert method == "GET"
    assert url == "https://api.permutive.com/cohorts"
    assert kwargs["params"] == {"k": "secret"}
    assert kwargs["timeout"] == (3.05, 30.0)


@pytest.mark.asyncio
async def test_async_request_reuses_sync_error_hierarchy() -> None:
    """Map asynchronous HTTP failures to canonical SDK errors."""
    transport = FakeTransport(FakeResponse(401, {"error": "unauthorized"}))
    client = AsyncPermutiveClient(
        "secret",
        transport=transport,
        retry_policy=RetryPolicy(max_attempts=1),
    )

    with pytest.raises(AuthenticationError) as captured:
        await client.request("GET", "cohorts")

    assert captured.value.status_code == 401
    assert captured.value.attempts == 1


@pytest.mark.asyncio
async def test_async_request_retries_without_blocking(monkeypatch: pytest.MonkeyPatch) -> None:
    """Retry safe requests through asynchronous sleep."""
    transport = FakeTransport(
        FakeResponse(503, {"error": "busy"}),
        FakeResponse(200, {"ok": True}),
    )
    client = AsyncPermutiveClient(
        "secret",
        transport=transport,
        retry_policy=RetryPolicy(max_attempts=2, initial_delay=0.01, jitter=0),
    )
    delays: List[float] = []

    async def record_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(client, "_sleep", record_sleep)

    assert await client.request("GET", "cohorts") == {"ok": True}
    assert delays == [0.01]
    assert len(transport.requests) == 2
