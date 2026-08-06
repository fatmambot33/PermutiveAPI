"""Shared credential rotation and rate-limit coordination primitives."""

from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Awaitable, Callable, Mapping, Optional, Protocol

from requests import Response


class SyncTransport(Protocol):
    """Minimal synchronous transport accepted by the coordinator wrapper."""

    def request(self, method: str, url: str, **kwargs: Any) -> Response:
        """Send one HTTP request."""
        ...


class AsyncResponseLike(Protocol):
    """Response fields needed by the asynchronous coordinator wrapper."""

    status_code: int
    headers: Mapping[str, str]


class AsyncTransportLike(Protocol):
    """Minimal asynchronous transport accepted by the coordinator wrapper."""

    async def request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> AsyncResponseLike:
        """Send one asynchronous HTTP request."""
        ...

    async def aclose(self) -> None:
        """Close asynchronous transport resources."""
        ...


@dataclass(frozen=True)
class CredentialSnapshot:
    """Contain one immutable credential generation for an in-flight request."""

    api_key: str
    generation: int

    def __repr__(self) -> str:
        """Return a representation that never exposes credential material."""
        return f"CredentialSnapshot(api_key='[REDACTED]', generation={self.generation})"


class AtomicCredentials:
    """Provide lock-protected credential snapshots and atomic rotation.

    Every transport attempt takes one snapshot before execution. Rotating the
    key therefore affects future attempts without changing credentials already
    held by an in-flight request.
    """

    def __init__(self, api_key: str) -> None:
        normalized = api_key.strip()
        if not normalized:
            raise ValueError("api_key must not be empty")
        self._api_key = normalized
        self._generation = 1
        self._lock = threading.RLock()

    def snapshot(self) -> CredentialSnapshot:
        """Return the current immutable credential generation."""
        with self._lock:
            return CredentialSnapshot(self._api_key, self._generation)

    def rotate(self, api_key: str) -> CredentialSnapshot:
        """Atomically replace the key and return the new generation."""
        normalized = api_key.strip()
        if not normalized:
            raise ValueError("api_key must not be empty")
        with self._lock:
            self._api_key = normalized
            self._generation += 1
            return CredentialSnapshot(self._api_key, self._generation)

    @property
    def generation(self) -> int:
        """Return the current credential generation without exposing the key."""
        with self._lock:
            return self._generation

    def __repr__(self) -> str:
        """Return a secret-safe representation."""
        return f"AtomicCredentials(api_key='[REDACTED]', generation={self.generation})"


class RateLimitCoordinator:
    """Coordinate request spacing and server deferrals across client types.

    A single coordinator can be shared by synchronous and asynchronous clients.
    Reservation is lock-protected, while sleeping happens outside the lock.
    """

    def __init__(
        self,
        requests_per_second: float = 10.0,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
        async_sleeper: Optional[Callable[[float], Awaitable[None]]] = None,
    ) -> None:
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be positive")
        self._interval = 1.0 / requests_per_second
        self._clock = clock
        self._sleeper = sleeper
        self._async_sleeper = async_sleeper or asyncio.sleep
        self._next_allowed = 0.0
        self._blocked_until = 0.0
        self._lock = threading.Lock()

    def reserve_delay(self) -> float:
        """Reserve one request slot and return the required delay in seconds."""
        with self._lock:
            now = self._clock()
            scheduled = max(now, self._next_allowed, self._blocked_until)
            self._next_allowed = scheduled + self._interval
            return max(0.0, scheduled - now)

    def acquire(self) -> float:
        """Wait synchronously for one coordinated request slot."""
        delay = self.reserve_delay()
        if delay:
            self._sleeper(delay)
        return delay

    async def acquire_async(self) -> float:
        """Wait asynchronously for one coordinated request slot."""
        delay = self.reserve_delay()
        if delay:
            await self._async_sleeper(delay)
        return delay

    def defer(self, seconds: float) -> None:
        """Block future reservations for at least ``seconds`` from now."""
        if seconds < 0:
            raise ValueError("seconds must be non-negative")
        with self._lock:
            self._blocked_until = max(
                self._blocked_until,
                self._clock() + seconds,
            )

    def observe_retry_after(self, value: Optional[str]) -> Optional[float]:
        """Parse a Retry-After value, coordinate the delay, and return seconds."""
        if value is None:
            return None
        delay = _retry_after_seconds(value)
        if delay is not None:
            self.defer(delay)
        return delay

    def reset(self) -> None:
        """Clear coordinated timing state, primarily for deterministic tests."""
        with self._lock:
            self._next_allowed = 0.0
            self._blocked_until = 0.0


class CoordinatedTransport:
    """Inject rotating credentials and shared pacing into a sync transport."""

    def __init__(
        self,
        transport: SyncTransport,
        credentials: AtomicCredentials,
        coordinator: RateLimitCoordinator,
    ) -> None:
        self._transport = transport
        self.credentials = credentials
        self.coordinator = coordinator

    def request(self, method: str, url: str, **kwargs: Any) -> Response:
        """Reserve a slot, inject one credential snapshot, and observe deferrals."""
        self.coordinator.acquire()
        snapshot = self.credentials.snapshot()
        kwargs["params"] = _credential_params(kwargs.get("params"), snapshot.api_key)
        response = self._transport.request(method, url, **kwargs)
        if response.status_code == 429:
            self.coordinator.observe_retry_after(response.headers.get("Retry-After"))
        return response

    def close(self) -> None:
        """Close the wrapped transport when it exposes a close method."""
        close = getattr(self._transport, "close", None)
        if callable(close):
            close()


class CoordinatedAsyncTransport:
    """Inject rotating credentials and shared pacing into an async transport."""

    def __init__(
        self,
        transport: AsyncTransportLike,
        credentials: AtomicCredentials,
        coordinator: RateLimitCoordinator,
    ) -> None:
        self._transport = transport
        self.credentials = credentials
        self.coordinator = coordinator

    async def request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> AsyncResponseLike:
        """Reserve a slot, inject one credential snapshot, and observe deferrals."""
        await self.coordinator.acquire_async()
        snapshot = self.credentials.snapshot()
        kwargs["params"] = _credential_params(kwargs.get("params"), snapshot.api_key)
        response = await self._transport.request(method, url, **kwargs)
        if response.status_code == 429:
            self.coordinator.observe_retry_after(response.headers.get("Retry-After"))
        return response

    async def aclose(self) -> None:
        """Close the wrapped asynchronous transport."""
        await self._transport.aclose()


def _credential_params(value: object, api_key: str) -> dict[str, object]:
    params: dict[str, object]
    if isinstance(value, Mapping):
        params = {str(key): item for key, item in value.items()}
    else:
        params = {}
    params["k"] = api_key
    return params


def _retry_after_seconds(value: str) -> Optional[float]:
    normalized = value.strip()
    if not normalized:
        return None
    try:
        return max(0.0, float(normalized))
    except ValueError:
        pass
    try:
        retry_at = parsedate_to_datetime(normalized)
    except (TypeError, ValueError, OverflowError):
        return None
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    return max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())


__all__ = [
    "AsyncResponseLike",
    "AsyncTransportLike",
    "AtomicCredentials",
    "CoordinatedAsyncTransport",
    "CoordinatedTransport",
    "CredentialSnapshot",
    "RateLimitCoordinator",
    "SyncTransport",
]
