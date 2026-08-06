"""Integration contracts for the deterministic local Permutive mock server."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from PermutiveAPI import (
    AsyncPermutiveClient,
    AuthenticationError,
    ConflictError,
    DecodingError,
    NotFoundError,
    PermutiveClient,
    RetryPolicy,
    ServerError,
    ValidationError,
)
from PermutiveAPI.testing import MockPermutiveServer, mock_fixture_catalog


@pytest.fixture
def retry_policy() -> RetryPolicy:
    """Return a fast deterministic retry policy for local HTTP tests."""
    return RetryPolicy(
        max_attempts=3,
        initial_delay=0.001,
        multiplier=1.0,
        max_delay=0.001,
        jitter=0.0,
    )


def test_fixture_catalog_matches_committed_version() -> None:
    """The reusable JSON fixture catalog cannot drift from package routes."""
    committed = json.loads(Path("mock_fixtures/v1.json").read_text(encoding="utf-8"))

    assert committed == mock_fixture_catalog()


def test_sync_client_success_and_request_capture(retry_policy: RetryPolicy) -> None:
    """The canonical client can use the loopback server without test adapters."""
    with MockPermutiveServer.standard() as server:
        with PermutiveClient(
            "test-api-key",
            base_url=server.base_url,
            retry_policy=retry_policy,
        ) as client:
            payload = client.request("GET", "v1/success")
            created = client.request(
                "POST",
                "v1/create",
                json={"name": "fixture"},
            )

    assert payload == {"id": "fixture-success", "state": "ready"}
    assert created == {"id": "fixture-created"}
    assert [request.path for request in server.requests] == [
        "/v1/success",
        "/v1/create",
    ]
    assert server.requests[0].query["k"] == ("test-api-key",)
    assert server.requests[1].body == {"name": "fixture"}


@pytest.mark.parametrize(
    ("method", "path", "error_type"),
    (
        ("POST", "v1/validation", ValidationError),
        ("GET", "v1/authentication", AuthenticationError),
        ("GET", "v1/not-found", NotFoundError),
        ("POST", "v1/conflict", ConflictError),
        ("GET", "v1/server-failure", ServerError),
    ),
)
def test_sync_error_mapping(
    method: str,
    path: str,
    error_type: type[Exception],
    retry_policy: RetryPolicy,
) -> None:
    """Representative fixture failures map through canonical SDK exceptions."""
    with MockPermutiveServer.standard() as server:
        with PermutiveClient(
            "test-api-key",
            base_url=server.base_url,
            retry_policy=retry_policy,
        ) as client:
            with pytest.raises(error_type):
                client.request(method, path)


def test_sync_retries_rate_limits_and_server_errors(
    retry_policy: RetryPolicy,
) -> None:
    """Queued responses deterministically exercise successful retries."""
    with MockPermutiveServer.standard() as server:
        with PermutiveClient(
            "test-api-key",
            base_url=server.base_url,
            retry_policy=retry_policy,
        ) as client:
            rate_limit = client.request("GET", "v1/rate-limit")
            server_retry = client.request("GET", "v1/server-retry")

    assert rate_limit == {"state": "recovered", "attempt": 2}
    assert server_retry == {"state": "recovered", "attempt": 2}
    assert [request.path for request in server.requests].count("/v1/rate-limit") == 2
    assert [request.path for request in server.requests].count("/v1/server-retry") == 2


def test_sync_pagination_and_repeated_token_guard(
    retry_policy: RetryPolicy,
) -> None:
    """The local fixtures cover complete and malformed continuation flows."""
    with MockPermutiveServer.standard() as server:
        with PermutiveClient(
            "test-api-key",
            base_url=server.base_url,
            retry_policy=retry_policy,
        ) as client:
            items = list(
                client.iter_all(
                    "v1/pagination",
                    item_decoder=lambda item: item,
                )
            )
            with pytest.raises(DecodingError, match="Repeated pagination"):
                list(
                    client.iter_all(
                        "v1/repeated-token",
                        item_decoder=lambda item: item,
                    )
                )

    assert items == [{"id": "page-1"}, {"id": "page-2"}]


@pytest.mark.asyncio
async def test_async_client_matches_sync_retry_and_capture(
    retry_policy: RetryPolicy,
) -> None:
    """The HTTPX async client observes the same local retry contract."""
    with MockPermutiveServer.standard() as server:
        async with AsyncPermutiveClient(
            "test-api-key",
            base_url=server.base_url,
            retry_policy=retry_policy,
        ) as client:
            success = await client.request("GET", "v1/success")
            recovered = await client.request("GET", "v1/rate-limit")

    assert success == {"id": "fixture-success", "state": "ready"}
    assert recovered == {"state": "recovered", "attempt": 2}
    assert [request.path for request in server.requests] == [
        "/v1/success",
        "/v1/rate-limit",
        "/v1/rate-limit",
    ]
