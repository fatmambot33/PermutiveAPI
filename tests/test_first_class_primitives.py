"""Contract tests for first-class SDK primitives."""

from __future__ import annotations

from typing import Optional

import pytest
from requests import Response

from PermutiveAPI import (
    BatchItemResult,
    BatchResult,
    ClientConfig,
    Page,
    PermutiveClient,
    iter_pages,
)


class FakeTransport:
    """Minimal deterministic transport for client tests."""

    def __init__(self, status: int = 200) -> None:
        self.status = status
        self.calls = []
        self.closed = False

    def request(self, method: str, url: str, **kwargs: object) -> Response:
        self.calls.append((method, url, kwargs))
        response = Response()
        response.status_code = self.status
        response.url = url
        response._content = b"{}"
        return response

    def close(self) -> None:
        self.closed = True


def test_client_has_explicit_timeout_and_api_key() -> None:
    transport = FakeTransport()
    client = PermutiveClient(ClientConfig(api_key="secret"), transport=transport)

    client.request("GET", "/v1/items")

    method, url, kwargs = transport.calls[0]
    assert method == "GET"
    assert url == "https://api.permutive.com/v1/items"
    assert kwargs["params"] == {"k": "secret"}
    assert kwargs["timeout"] == (3.05, 30.0)


def test_client_rejects_implicit_idempotent_writes() -> None:
    client = PermutiveClient(ClientConfig(api_key="secret"), transport=FakeTransport())
    with pytest.raises(ValueError, match="Unsafe writes"):
        client.request("POST", "/v1/items", idempotent=True)


def test_client_closes_in_context_manager() -> None:
    transport = FakeTransport()
    with PermutiveClient(ClientConfig(api_key="secret"), transport=transport):
        pass
    assert transport.closed


def test_iter_pages_is_lazy_and_bounded() -> None:
    calls = []

    def fetch(token: Optional[str]) -> Page[int]:
        calls.append(token)
        if token is None:
            return Page((1, 2), "next")
        return Page((3, 4), None)

    assert list(iter_pages(fetch, max_items=3)) == [1, 2, 3]
    assert calls == [None, "next"]


def test_iter_pages_rejects_repeated_tokens() -> None:
    def fetch(_: Optional[str]) -> Page[int]:
        return Page((1,), "same")

    with pytest.raises(RuntimeError, match="repeated"):
        list(iter_pages(fetch))


def test_batch_result_preserves_input_identity() -> None:
    result = BatchResult(
        (
            BatchItemResult("a", value=1),
            BatchItemResult("b", error=ValueError("bad")),
        )
    )
    assert [item.item for item in result.successes] == ["a"]
    assert [item.item for item in result.failures] == ["b"]
