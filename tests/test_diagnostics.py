"""Tests for framework-neutral request diagnostics."""

from __future__ import annotations

from typing import Any, List, Mapping, Optional

import pytest

from PermutiveAPI.diagnostics import (
    AsyncDiagnosticTransport,
    DiagnosticTransport,
    RequestDiagnostic,
)


class Response:
    """Small response double."""

    def __init__(
        self, status_code: int, headers: Optional[Mapping[str, str]] = None
    ) -> None:
        self.status_code = status_code
        self.headers = dict(headers or {})


class SyncTransport:
    """Ordered synchronous transport double."""

    def __init__(self, *responses: Response) -> None:
        self.responses = list(responses)
        self.closed = False

    def request(self, method: str, url: str, **kwargs: Any) -> Response:
        """Return the next response."""
        return self.responses.pop(0)

    def close(self) -> None:
        """Record closure."""
        self.closed = True


class AsyncTransport:
    """Ordered asynchronous transport double."""

    def __init__(self, *responses: Response) -> None:
        self.responses = list(responses)
        self.closed = False

    async def request(self, method: str, url: str, **kwargs: Any) -> Response:
        """Return the next response."""
        return self.responses.pop(0)

    async def aclose(self) -> None:
        """Record closure."""
        self.closed = True


def test_sync_diagnostics_are_safe_and_track_retries() -> None:
    """Emit safe metadata with retry attempts and request identifiers."""
    events: List[RequestDiagnostic] = []
    wrapped = DiagnosticTransport(
        SyncTransport(
            Response(429),
            Response(200, {"X-Request-ID": "request-2"}),
        ),
        events.append,
    )

    wrapped.request("get", "https://api.example.test/items?k=secret", json={"token": "x"})
    wrapped.request("get", "https://api.example.test/items?k=secret", json={"token": "x"})

    completed = [event for event in events if event.phase == "end"]
    assert [event.attempt for event in completed] == [1, 2]
    assert completed[-1].request_id == "request-2"
    assert all("secret" not in event.endpoint for event in events)
    assert all("token" not in repr(event) for event in events)


def test_sync_diagnostics_emit_error_type_without_message() -> None:
    """Avoid leaking exception messages into diagnostic metadata."""
    events: List[RequestDiagnostic] = []

    class FailingTransport:
        def request(self, method: str, url: str, **kwargs: Any) -> Response:
            raise RuntimeError("credential-secret")

    wrapped = DiagnosticTransport(FailingTransport(), events.append)
    with pytest.raises(RuntimeError, match="credential-secret"):
        wrapped.request("GET", "https://api.example.test/items")

    error = events[-1]
    assert error.phase == "error"
    assert error.error_type == "RuntimeError"
    assert "credential-secret" not in repr(error)


@pytest.mark.asyncio
async def test_async_diagnostics_match_sync_contract() -> None:
    """Emit equivalent metadata for asynchronous transports."""
    events: List[RequestDiagnostic] = []
    transport = AsyncTransport(Response(204, {"Request-ID": "async-1"}))
    wrapped = AsyncDiagnosticTransport(transport, events.append)

    response = await wrapped.request("delete", "https://api.example.test/items/1?k=x")
    await wrapped.aclose()

    assert response.status_code == 204
    assert events[-1].method == "DELETE"
    assert events[-1].request_id == "async-1"
    assert transport.closed
