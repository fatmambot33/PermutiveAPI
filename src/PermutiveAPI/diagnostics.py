"""Framework-neutral request diagnostics for sync and async transports."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Mapping, Optional, Protocol, Tuple
from urllib.parse import urlsplit, urlunsplit


@dataclass(frozen=True)
class RequestDiagnostic:
    """Safe structured metadata for one transport attempt."""

    phase: str
    method: str
    endpoint: str
    attempt: int
    duration: float
    status_code: Optional[int] = None
    request_id: Optional[str] = None
    error_type: Optional[str] = None


DiagnosticHook = Callable[[RequestDiagnostic], None]


class SyncResponse(Protocol):
    """Response fields required by the diagnostic wrapper."""

    status_code: int
    headers: Mapping[str, str]


class SyncTransport(Protocol):
    """Synchronous transport accepted by :class:`DiagnosticTransport`."""

    def request(self, method: str, url: str, **kwargs: Any) -> SyncResponse:
        """Send one request."""
        ...


class AsyncResponse(Protocol):
    """Asynchronous response fields required by the diagnostic wrapper."""

    status_code: int
    headers: Mapping[str, str]


class AsyncTransport(Protocol):
    """Asynchronous transport accepted by :class:`AsyncDiagnosticTransport`."""

    async def request(self, method: str, url: str, **kwargs: Any) -> AsyncResponse:
        """Send one asynchronous request."""
        ...

    async def aclose(self) -> None:
        """Close transport resources."""
        ...


def _safe_endpoint(url: str) -> str:
    """Remove query parameters and fragments from a diagnostic endpoint."""
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def _request_id(headers: Mapping[str, str]) -> Optional[str]:
    """Extract a common request identifier header."""
    return headers.get("X-Request-ID") or headers.get("Request-ID")


def _is_retryable(status_code: int) -> bool:
    """Return whether a response commonly participates in SDK retries."""
    return status_code == 429 or status_code >= 500


class DiagnosticTransport:
    """Wrap a synchronous transport and emit safe request metadata."""

    def __init__(self, transport: SyncTransport, hook: DiagnosticHook) -> None:
        self._transport = transport
        self._hook = hook
        self._attempts: Dict[Tuple[str, str], int] = {}

    def request(self, method: str, url: str, **kwargs: Any) -> SyncResponse:
        """Delegate one request while emitting start and completion events."""
        verb = method.upper()
        endpoint = _safe_endpoint(url)
        key = (verb, endpoint)
        attempt = self._attempts.get(key, 0) + 1
        self._attempts[key] = attempt
        started = time.monotonic()
        self._hook(RequestDiagnostic("start", verb, endpoint, attempt, 0.0))
        try:
            response = self._transport.request(method, url, **kwargs)
        except Exception as exc:
            self._hook(
                RequestDiagnostic(
                    "error",
                    verb,
                    endpoint,
                    attempt,
                    time.monotonic() - started,
                    error_type=type(exc).__name__,
                )
            )
            raise
        self._hook(
            RequestDiagnostic(
                "end",
                verb,
                endpoint,
                attempt,
                time.monotonic() - started,
                status_code=response.status_code,
                request_id=_request_id(response.headers),
            )
        )
        if not _is_retryable(response.status_code):
            self._attempts.pop(key, None)
        return response

    def close(self) -> None:
        """Close the wrapped transport when supported."""
        close = getattr(self._transport, "close", None)
        if callable(close):
            close()


class AsyncDiagnosticTransport:
    """Wrap an asynchronous transport and emit safe request metadata."""

    def __init__(self, transport: AsyncTransport, hook: DiagnosticHook) -> None:
        self._transport = transport
        self._hook = hook
        self._attempts: Dict[Tuple[str, str], int] = {}

    async def request(self, method: str, url: str, **kwargs: Any) -> AsyncResponse:
        """Delegate one request while emitting start and completion events."""
        verb = method.upper()
        endpoint = _safe_endpoint(url)
        key = (verb, endpoint)
        attempt = self._attempts.get(key, 0) + 1
        self._attempts[key] = attempt
        started = time.monotonic()
        self._hook(RequestDiagnostic("start", verb, endpoint, attempt, 0.0))
        try:
            response = await self._transport.request(method, url, **kwargs)
        except Exception as exc:
            self._hook(
                RequestDiagnostic(
                    "error",
                    verb,
                    endpoint,
                    attempt,
                    time.monotonic() - started,
                    error_type=type(exc).__name__,
                )
            )
            raise
        self._hook(
            RequestDiagnostic(
                "end",
                verb,
                endpoint,
                attempt,
                time.monotonic() - started,
                status_code=response.status_code,
                request_id=_request_id(response.headers),
            )
        )
        if not _is_retryable(response.status_code):
            self._attempts.pop(key, None)
        return response

    async def aclose(self) -> None:
        """Close the wrapped asynchronous transport."""
        await self._transport.aclose()


__all__ = [
    "AsyncDiagnosticTransport",
    "DiagnosticHook",
    "DiagnosticTransport",
    "RequestDiagnostic",
]
