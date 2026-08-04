"""Deterministic tests for the 6.4 core capability slice."""

from __future__ import annotations

from typing import Any, Mapping

import pytest

from PermutiveAPI import (
    AsyncPermutiveClient,
    PermutiveConfig,
    Secret,
    all_of,
    event,
    in_segment,
    property_condition,
)


class StubResponse:
    """Minimal response used by async transport tests."""

    def __init__(self, status_code: int, payload: Any) -> None:
        self.status_code = status_code
        self.headers: Mapping[str, str] = {}
        self._payload = payload

    def json(self) -> Any:
        """Return the configured payload."""
        return self._payload


class StubTransport:
    """Minimal asynchronous transport used by tests."""

    def __init__(self, response: StubResponse) -> None:
        self.response = response
        self.closed = False
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    async def request(self, method: str, url: str, **kwargs: Any) -> StubResponse:
        """Record and return one response."""
        self.calls.append((method, url, kwargs))
        return self.response

    async def aclose(self) -> None:
        """Record closure."""
        self.closed = True


def test_secret_is_redacted() -> None:
    """Ensure credentials cannot leak through string representations."""
    secret = Secret("top-secret")
    assert "top-secret" not in repr(secret)
    assert "top-secret" not in str(secret)


def test_config_rejects_insecure_remote_url() -> None:
    """Ensure remote endpoints require TLS."""
    with pytest.raises(ValueError, match="HTTPS"):
        PermutiveConfig(api_key=Secret("key"), base_url="http://example.com")


def test_config_allows_explicit_local_development() -> None:
    """Ensure local HTTP requires explicit opt-in."""
    config = PermutiveConfig(
        api_key=Secret("key"),
        base_url="http://localhost:8000",
        allow_insecure_localhost=True,
    )
    assert config.base_url == "http://localhost:8000"


def test_query_builder_serializes_nested_expressions() -> None:
    """Ensure typed builders produce deterministic native payloads."""
    expression = all_of(
        [
            event("pageview"),
            property_condition("url", "contains", "example.com") | in_segment(42),
        ]
    )
    assert expression.to_json() == {
        "and": [
            {
                "event": "pageview",
                "frequency": {"greater_than_or_equal_to": 1},
            },
            {
                "or": [
                    {"property": "url", "condition": {"contains": "example.com"}},
                    {"in_segment": 42},
                ]
            },
        ]
    }


@pytest.mark.asyncio
async def test_async_client_uses_injected_transport_and_closes() -> None:
    """Ensure async requests are injectable, typed, and close cleanly."""
    transport = StubTransport(StubResponse(200, {"id": "cohort-1"}))
    client = AsyncPermutiveClient("secret", transport=transport)

    result = await client.request("GET", "cohorts-api/v2/cohorts/cohort-1")
    await client.close()

    assert result == {"id": "cohort-1"}
    assert transport.closed
    assert transport.calls[0][0] == "GET"
    assert transport.calls[0][2]["params"]["k"] == "secret"
