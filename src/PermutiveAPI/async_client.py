"""Typed asynchronous Permutive API client and parity helpers."""

from __future__ import annotations

import asyncio
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Generic,
    List,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    TypeVar,
    cast,
)

from .sdk import (
    BatchItem,
    BatchResult,
    DecodingError,
    JSONObject,
    JSONScalar,
    Page,
    RetryPolicy,
    TransportError,
    _error_for_response,  # pyright: ignore[reportPrivateUsage]
    _redact,  # pyright: ignore[reportPrivateUsage]
)

T = TypeVar("T")
R = TypeVar("R")


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

    async def list_page(
        self,
        path: str,
        *,
        item_decoder: Callable[[JSONObject], T],
        page_size: int = 100,
        continuation: Optional[str] = None,
    ) -> Page[T]:
        """Fetch one typed page using synchronous continuation semantics."""
        if page_size < 1:
            raise ValueError("page_size must be positive")
        params: Dict[str, JSONScalar] = {"limit": page_size}
        if continuation:
            params["continuation"] = continuation
        payload = await self.request("GET", path, params=params)
        raw_items = payload.get("items", [])
        if not isinstance(raw_items, list):
            raise DecodingError("Paginated response field 'items' must be a list")
        items = tuple(
            item_decoder(cast(JSONObject, item))
            for item in raw_items
            if isinstance(item, dict)
        )
        token = payload.get("continuation") or payload.get("next_token")
        return Page(items=items, next_token=token if isinstance(token, str) else None)

    async def iter_all(
        self,
        path: str,
        *,
        item_decoder: Callable[[JSONObject], T],
        page_size: int = 100,
        max_items: Optional[int] = None,
    ) -> AsyncIterator[T]:
        """Lazily iterate pages with maximum-item and token guards."""
        if max_items is not None and max_items < 0:
            raise ValueError("max_items must be zero or greater")
        token: Optional[str] = None
        seen: set[str] = set()
        yielded = 0
        while True:
            page = await self.list_page(
                path,
                item_decoder=item_decoder,
                page_size=page_size,
                continuation=token,
            )
            for item in page.items:
                if max_items is not None and yielded >= max_items:
                    return
                yielded += 1
                yield item
            if page.next_token is None:
                return
            if page.next_token in seen:
                raise DecodingError("Repeated pagination continuation token")
            seen.add(page.next_token)
            token = page.next_token

    async def _sleep(self, delay: float) -> None:
        """Sleep without blocking the event loop."""
        await asyncio.sleep(min(delay, self._retry.max_delay))


class AsyncResource(Generic[T]):
    """Typed asynchronous CRUD facade over the canonical client."""

    def __init__(
        self,
        client: AsyncPermutiveClient,
        path: str,
        decoder: Callable[[JSONObject], T],
    ) -> None:
        self._client = client
        self._path = path.strip("/")
        self._decoder = decoder

    async def get(self, resource_id: str) -> T:
        """Fetch and decode one resource."""
        return self._decoder(
            await self._client.request("GET", f"{self._path}/{resource_id}")
        )

    async def create(self, payload: JSONObject) -> T:
        """Create and decode one resource without implicit retries."""
        return self._decoder(
            await self._client.request("POST", self._path, json=payload)
        )

    async def update(self, resource_id: str, payload: JSONObject) -> T:
        """Update and decode one resource without implicit retries."""
        return self._decoder(
            await self._client.request(
                "PATCH", f"{self._path}/{resource_id}", json=payload
            )
        )

    async def delete(self, resource_id: str) -> None:
        """Delete one resource."""
        await self._client.request("DELETE", f"{self._path}/{resource_id}")

    async def list_page(
        self,
        *,
        page_size: int = 100,
        continuation: Optional[str] = None,
    ) -> Page[T]:
        """Fetch one typed resource page."""
        return await self._client.list_page(
            self._path,
            item_decoder=self._decoder,
            page_size=page_size,
            continuation=continuation,
        )

    async def iter_all(
        self,
        *,
        page_size: int = 100,
        max_items: Optional[int] = None,
    ) -> AsyncIterator[T]:
        """Iterate all decoded resources lazily."""
        async for item in self._client.iter_all(
            self._path,
            item_decoder=self._decoder,
            page_size=page_size,
            max_items=max_items,
        ):
            yield item


async def execute_async_batch(
    inputs: Sequence[T],
    operation: Callable[[T], Awaitable[R]],
    *,
    max_concurrency: int = 4,
    fail_fast: bool = False,
    progress: Optional[Callable[[int, int], None]] = None,
) -> BatchResult[T, R]:
    """Execute ordered asynchronous work with bounded concurrency.

    Cancellation propagates after all child tasks are cancelled and collected.
    Partial failures are returned as ordered :class:`BatchItem` values unless
    ``fail_fast`` is enabled.
    """
    if max_concurrency < 1:
        raise ValueError("max_concurrency must be positive")
    if not inputs:
        return BatchResult(items=())

    semaphore = asyncio.Semaphore(max_concurrency)
    outcomes: List[Optional[BatchItem[T, R]]] = [None] * len(inputs)
    completed = 0

    async def run(index: int, value: T) -> None:
        nonlocal completed
        async with semaphore:
            try:
                result = await operation(value)
                outcomes[index] = BatchItem(input=value, value=result)
            except Exception as exc:
                outcomes[index] = BatchItem(input=value, error=exc)
                if fail_fast:
                    raise
            finally:
                completed += 1
                if progress is not None:
                    progress(completed, len(inputs))

    tasks = [
        asyncio.create_task(run(index, value)) for index, value in enumerate(inputs)
    ]
    try:
        await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise

    return BatchResult(items=tuple(cast(BatchItem[T, R], item) for item in outcomes))


__all__ = [
    "AsyncPermutiveClient",
    "AsyncResource",
    "AsyncResponse",
    "AsyncTransport",
    "execute_async_batch",
]
