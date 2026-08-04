"""Contract tests for the simplified public client."""

from __future__ import annotations

import json
from typing import Any

from requests import Response

from PermutiveAPI import PermutiveClient


class FakeTransport:
    """Deterministic transport for resource facade tests."""

    def __init__(self, payloads: list[object]) -> None:
        self.payloads = payloads
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def request(self, method: str, url: str, **kwargs: Any) -> Response:
        """Return the next configured response."""
        self.calls.append((method, url, kwargs))
        result = Response()
        result.status_code = 200
        result._content = json.dumps(self.payloads.pop(0)).encode()
        return result


def test_package_client_exposes_predictable_resource_namespaces() -> None:
    """Expose one obvious operation surface for every primary resource."""
    transport = FakeTransport([{"id": "one"}])
    client = PermutiveClient("secret", transport=transport)

    assert client.cohorts.get("one") == {"id": "one"}
    assert transport.calls[0][0] == "GET"
    assert transport.calls[0][1].endswith("/cohorts-api/v2/cohorts/one")
    assert client.cohorts is client.cohorts


def test_resource_list_uses_the_same_method_name_everywhere() -> None:
    """List resources without exposing pagination implementation details."""
    transport = FakeTransport([{"items": [{"id": "one"}]}])
    client = PermutiveClient("secret", transport=transport)

    page = client.segments.list(page_size=25)

    assert page.items == ({"id": "one"},)
    assert transport.calls[0][2]["params"]["limit"] == 25
