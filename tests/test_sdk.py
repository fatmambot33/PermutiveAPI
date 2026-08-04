"""Contract tests for the stable SDK primitives."""

from __future__ import annotations

import json
from typing import Any

import pytest
import requests
from requests import Response

from PermutiveAPI.sdk import (
    AuthenticationError,
    DecodingError,
    PermutiveClient,
    RetryPolicy,
    execute_batch,
)


class FakeTransport:
    """Deterministic transport returning configured responses."""

    def __init__(self, responses: list[Response | Exception]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def request(self, method: str, url: str, **kwargs: Any) -> Response:
        """Return the next configured response."""
        self.calls.append((method, url, kwargs))
        current = self.responses.pop(0)
        if isinstance(current, Exception):
            raise current
        return current


def response(status: int, payload: object = None, **headers: str) -> Response:
    """Build one requests response fixture."""
    result = Response()
    result.status_code = status
    result.headers.update(headers)
    result._content = b"" if payload is None else json.dumps(payload).encode()
    return result


def client(transport: FakeTransport) -> PermutiveClient:
    """Build a no-delay test client."""
    instance = PermutiveClient(
        "secret",
        transport=transport,
        retry_policy=RetryPolicy(
            max_attempts=2,
            initial_delay=0.001,
            max_delay=0.001,
            jitter=0,
        ),
    )
    instance._sleep = lambda _: None  # type: ignore[method-assign]
    return instance


def test_request_decodes_object_and_uses_explicit_timeout() -> None:
    """Decode objects while forwarding timeout and credentials."""
    transport = FakeTransport([response(200, {"id": "one"})])
    result = client(transport).request("GET", "/cohorts")
    assert result == {"id": "one"}
    assert transport.calls[0][2]["timeout"] == (3.05, 30.0)
    assert transport.calls[0][2]["params"]["k"] == "secret"


def test_unsafe_post_is_not_retried_by_default() -> None:
    """Avoid retrying unsafe writes without explicit idempotency."""
    transport = FakeTransport([response(503), response(200, {})])
    with pytest.raises(Exception):
        client(transport).request("POST", "/cohorts", json={})
    assert len(transport.calls) == 1


def test_safe_get_retries_transient_failure() -> None:
    """Retry transient failures for safe reads."""
    transport = FakeTransport([response(503), response(200, {"ok": True})])
    assert client(transport).request("GET", "/cohorts") == {"ok": True}
    assert len(transport.calls) == 2


def test_status_mapping_is_typed() -> None:
    """Map authentication status codes to focused errors."""
    with pytest.raises(AuthenticationError):
        client(FakeTransport([response(401)])).request("GET", "/cohorts")


def test_transport_error_redacts_secret() -> None:
    """Remove credentials from transport error messages."""
    failure = requests.ConnectionError("failed https://example.test?k=secret")
    with pytest.raises(Exception) as caught:
        client(FakeTransport([failure, failure])).request("GET", "/cohorts")
    assert "secret" not in str(caught.value)


def test_pagination_and_repeated_token_protection() -> None:
    """Stop iteration when a continuation token repeats."""
    transport = FakeTransport(
        [
            response(200, {"items": [{"id": "1"}], "continuation": "same"}),
            response(200, {"items": [{"id": "2"}], "continuation": "same"}),
        ]
    )
    iterator = client(transport).iter_all(
        "/cohorts",
        item_decoder=lambda item: item["id"],
    )
    assert next(iterator) == "1"
    assert next(iterator) == "2"
    with pytest.raises(DecodingError):
        next(iterator)


def test_batch_preserves_input_order_and_collects_failures() -> None:
    """Return ordered batch outcomes with item-level failures."""

    def operation(value: int) -> int:
        if value == 2:
            raise ValueError("bad")
        return value * 10

    progress: list[tuple[int, int]] = []
    result = execute_batch(
        [3, 2, 1],
        operation,
        progress=lambda done, total: progress.append((done, total)),
    )
    assert [item.input for item in result.items] == [3, 2, 1]
    assert [item.value for item in result.successes] == [30, 10]
    assert len(result.failures) == 1
    assert progress[-1] == (3, 3)
