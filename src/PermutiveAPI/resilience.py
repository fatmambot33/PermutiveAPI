"""Shared credential rotation and rate-limit coordination primitives."""

from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Awaitable, Callable, Optional


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

    Every request takes one snapshot before transport execution. Rotating the
    key therefore affects future requests without changing credentials already
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
    "AtomicCredentials",
    "CredentialSnapshot",
    "RateLimitCoordinator",
]
