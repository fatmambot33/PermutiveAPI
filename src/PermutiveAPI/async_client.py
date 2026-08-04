"""Typed asynchronous Permutive API client."""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional, Protocol, Tuple, cast

from .sdk import (
    DecodingError,
    JSONObject,
    JSONScalar,
    RetryPolicy,
    TransportError,
    _error_for_response,  # pyright: ignore[reportPrivateUsage]
    _redact,  # pyright: ignore[reportPrivateUsage]
)


class AsyncResponse(Protocol):
    """Minimal asynchronous response contract."""

    status_code: int
    content: bytes
    headers: Mapping[str, str]

    def json(self) -> Any:
        """Decode the response body."""
        ...


class AsyncTransport(Protocol):
    """Minimal injectable asynchronous transport contract."""

    async def request(self, method: str, url: str, **kwargs: Any) -> AsyncResponse:
        """Send one asynchronous HTTP request."""
        ...

    async def aclose(self) -> None:
        """Close transport resources."""
        ...


class AsyncPermutiveClient:
    """Asynchronous dependency-injectable Permutive API client.

    The default transport requires the optional ``async`` extra. Tests and
    applications may inject any transport implementing :class:`AsyncTransport`.
    """

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = "https://api.permutive.com",
        timeout: Tuple[float, float] = (3.05, 30.0),
        retry_policy: Optional[RetryPolicy] = None,
        transport: Optional[AsyncTransport] = None,
    ) -> None:
        if not api_key:
            raise ValueError("api_key must not be empty")
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._retry = retry_policy or RetryPolicy()
        self._transport = transport or self._default_transport(timeout)

    @staticmethod
    def _default_transport(timeout: Tuple[float, float]) -> AsyncTransport:
        """Create the optional HTTPX transport lazily."""
        try:
            import httpx
        except ImportError as exc:
            raise ImportError(
                "AsyncPermutiveClient requires 'PermutiveAPI[async]' or an "
                "injected AsyncTransport"
            ) from exc
        return cast(
            AsyncTransport,
            httpx.AsyncClient(
                timeout=httpx.Timeout(
                    connect=timeout[0],
                    read=timeout[1],
                    write=timeout[1],
                    pool=timeout[0],
                )
            ),
        )

    async def close(self) -> None:
        """Close the underlying asynchronous transport."""
        await self._transport.aclose()

    async def __aenter__(self) -> "AsyncPermutiveClient":
        """Return the active client context."""
        return self

    async def __aexit__(self, *_: object) -> None:
        """Close the active client context."""
        await self.close()

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Mapping[str, JSONScalar]] = None,
        json: Optional[JSONObject] = None,
        idempotent: Optional[bool] = None,
    ) -> JSONObject:
        """Send an asynchronous request and decode one JSON object."""
        verb = method.upper()
        safe = verb in {"GET", "HEAD", "OPTIONS", "DELETE"}
        may_retry = safe if idempotent is None else idempotent
        endpoint = f"{self._base_url}/{path.lstrip('/')}"
        query: Dict[str, JSONScalar] = dict(params or {})
        query["k"] = self._api_key
        delay = self._retry.initial_delay

        for attempt in range(1, self._retry.max_attempts + 1):
            try:
                response = await self._transport.request(
                    verb,
                    endpoint,
                    params=query,
                    json=json,
                    headers={
                        "Accept": "application/json",
                        "Content-Type": "application/json",
                    },
                    timeout=self._timeout,
                )
            except Exception as exc:
                if not may_retry or attempt == self._retry.max_attempts:
                    raise TransportError(
                        _redact(str(exc), self._api_key),
                        endpoint=endpoint,
                        retryable=may_retry,
                        attempts=attempt,
                    ) from exc
                await self._sleep(delay)
                delay = min(delay * self._retry.multiplier, self._retry.max_delay)
                continue

            if 200 <= response.status_code < 300:
                if response.status_code == 204 or not response.content:
                    return {}
                try:
                    payload = response.json()
                except ValueError as exc:
                    raise DecodingError(
                        "Permutive returned invalid JSON",
                        status_code=response.status_code,
                        endpoint=endpoint,
                        attempts=attempt,
                    ) from exc
                if not isinstance(payload, dict):
                    raise DecodingError(
                        "Permutive returned a non-object JSON payload",
                        status_code=response.status_code,
                        endpoint=endpoint,
                        attempts=attempt,
                    )
                return cast(JSONObject, payload)

            retryable = response.status_code in self._retry.retry_statuses
            if not may_retry or not retryable or attempt == self._retry.max_attempts:
                raise _error_for_response(cast(Any, response), endpoint, attempt)
            retry_after = response.headers.get("Retry-After")
            if retry_after is not None:
                try:
                    delay = min(float(retry_after), self._retry.max_delay)
                except ValueError:
                    pass
            await self._sleep(delay)
            delay = min(delay * self._retry.multiplier, self._retry.max_delay)

        raise TransportError("Request terminated unexpectedly", endpoint=endpoint)

    async def _sleep(self, delay: float) -> None:
        """Sleep without blocking the event loop."""
        import asyncio

        await asyncio.sleep(min(delay, self._retry.max_delay))
