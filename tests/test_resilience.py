"""Tests for coordinated rate limiting and atomic credential rotation."""

from __future__ import annotations

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Mapping

import pytest
from requests import Response

from PermutiveAPI.resilience import (
    AtomicCredentials,
    CoordinatedAsyncTransport,
    CoordinatedTransport,
    RateLimitCoordinator,
)
from PermutiveAPI.sdk import PermutiveClient, RetryPolicy


class FakeClock:
    """Deterministic monotonic clock and sleeper."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, delay: float) -> None:
        self.sleeps.append(delay)
        self.now += delay

    async def sleep_async(self, delay: float) -> None:
        self.sleep(delay)


class CapturingTransport:
    """Capture request parameters and return queued HTTP statuses."""

    def __init__(self, statuses: tuple[int, ...] = (200,)) -> None:
        self.statuses = list(statuses)
        self.params: list[dict[str, object]] = []

    def request(self, method: str, url: str, **kwargs: Any) -> Response:
        del method
        self.params.append(dict(kwargs["params"]))
        response = Response()
        response.status_code = self.statuses.pop(0)
        response.url = url
        response.headers["Content-Type"] = "application/json"
        if response.status_code == 429:
            response.headers["Retry-After"] = "2"
        response._content = json.dumps({"ok": True}).encode()
        return response

    def close(self) -> None:
        pass


class AsyncCapturingTransport:
    """Capture asynchronous request parameters and act as a response."""

    def __init__(self) -> None:
        self.params: list[dict[str, object]] = []
        self.content = b'{"ok": true}'

    async def request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> "AsyncCapturingTransport":
        del method, url
        self.params.append(dict(kwargs["params"]))
        return self

    @property
    def status_code(self) -> int:
        return 200

    @property
    def headers(self) -> Mapping[str, str]:
        return {}

    def json(self) -> dict[str, bool]:
        return {"ok": True}

    async def aclose(self) -> None:
        pass


def test_shared_coordinator_reserves_non_overlapping_slots() -> None:
    """Sync and async callers use the same deterministic reservation state."""
    clock = FakeClock()
    coordinator = RateLimitCoordinator(
        2.0,
        clock=clock,
        sleeper=clock.sleep,
        async_sleeper=clock.sleep_async,
    )

    assert coordinator.acquire() == 0.0
    assert coordinator.acquire() == pytest.approx(0.5)
    assert asyncio.run(coordinator.acquire_async()) == pytest.approx(0.5)
    assert clock.sleeps == [0.5, 0.5]


def test_atomic_rotation_changes_future_attempts_only() -> None:
    """Transport attempts take immutable generations and never expose secrets."""
    credentials = AtomicCredentials("first-secret")
    first = credentials.snapshot()
    second = credentials.rotate("second-secret")

    assert first.api_key == "first-secret"
    assert first.generation == 1
    assert second.api_key == "second-secret"
    assert second.generation == 2
    assert "first-secret" not in repr(credentials)
    assert "second-secret" not in repr(second)


def test_coordinated_transport_replaces_placeholder_key_after_rotation() -> None:
    """The wrapper injects the current atomic key at the transport boundary."""
    raw = CapturingTransport((200, 200))
    credentials = AtomicCredentials("first-secret")
    coordinator = RateLimitCoordinator(1000.0, sleeper=lambda delay: None)
    transport = CoordinatedTransport(raw, credentials, coordinator)
    with PermutiveClient(
        "managed-placeholder",
        base_url="https://example.test",
        retry_policy=RetryPolicy(max_attempts=1),
        transport=transport,
    ) as client:
        client.request("GET", "v1/cohorts")
        credentials.rotate("second-secret")
        client.request("GET", "v1/cohorts")

    assert raw.params[0]["k"] == "first-secret"
    assert raw.params[1]["k"] == "second-secret"
    assert all(value["k"] != "managed-placeholder" for value in raw.params)


def test_rate_limit_response_defers_all_future_callers() -> None:
    """A Retry-After response updates the shared coordinator once."""
    clock = FakeClock()
    coordinator = RateLimitCoordinator(
        10.0,
        clock=clock,
        sleeper=clock.sleep,
        async_sleeper=clock.sleep_async,
    )
    raw = CapturingTransport((429,))
    transport = CoordinatedTransport(raw, AtomicCredentials("secret"), coordinator)

    response = transport.request("GET", "https://example.test/v1/cohorts", params={})
    assert response.status_code == 429
    assert coordinator.reserve_delay() == pytest.approx(2.0)


def test_atomic_credentials_remain_consistent_under_rotation_stress() -> None:
    """Concurrent readers observe complete generations, never partial values."""
    credentials = AtomicCredentials("secret-0")

    def read_many(_: int) -> tuple[str, int]:
        snapshot = credentials.snapshot()
        return snapshot.api_key, snapshot.generation

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = []
        for index in range(1, 51):
            credentials.rotate(f"secret-{index}")
            futures.extend(executor.submit(read_many, item) for item in range(4))
    snapshots = [future.result() for future in futures]

    assert all(key == f"secret-{generation - 1}" for key, generation in snapshots)
    assert credentials.generation == 51


@pytest.mark.asyncio
async def test_async_wrapper_injects_current_credentials() -> None:
    """The asynchronous wrapper shares the same credentials and limiter."""
    raw = AsyncCapturingTransport()
    credentials = AtomicCredentials("async-secret")
    coordinator = RateLimitCoordinator(1000.0)
    transport = CoordinatedAsyncTransport(raw, credentials, coordinator)

    await transport.request("GET", "https://example.test/v1/cohorts", params={})
    credentials.rotate("rotated-async-secret")
    await transport.request("GET", "https://example.test/v1/cohorts", params={})

    assert [params["k"] for params in raw.params] == [
        "async-secret",
        "rotated-async-secret",
    ]


@pytest.mark.asyncio
async def test_async_cancellation_propagates_without_retry_or_secret_leak() -> None:
    """Cancellation interrupts a pending transport request immediately."""

    class BlockingTransport:
        def __init__(self) -> None:
            self.started = asyncio.Event()
            self.release = asyncio.Event()

        async def request(
            self,
            method: str,
            url: str,
            **kwargs: Any,
        ) -> AsyncCapturingTransport:
            del method, url, kwargs
            self.started.set()
            await self.release.wait()
            return AsyncCapturingTransport()

        async def aclose(self) -> None:
            pass

    raw = BlockingTransport()
    transport = CoordinatedAsyncTransport(
        raw,
        AtomicCredentials("never-visible"),
        RateLimitCoordinator(1000.0),
    )
    task = asyncio.create_task(
        transport.request("GET", "https://example.test/v1/cohorts", params={})
    )
    await raw.started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
