"""Asynchronous client with parity with the canonical synchronous SDK."""

from __future__ import annotations

import asyncio
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Dict,
    Generic,
    Mapping,
    Optional,
    Protocol,
    Tuple,
    TypeVar,
    Union,
    cast,
)

from .config import PermutiveConfig, Secret
from .sdk import DecodingError, JSONObject, JSONScalar, Page, RetryPolicy, SDKError

T = TypeVar("T")


class AsyncResponse(Protocol):
    """Minimal asynchronous response contract."""

    status_code: int
    headers: Mapping[str, str]

    def json(self) -> Any:
        """Decode response JSON."""
        ...


class AsyncTransport(Protocol):
    """Minimal injectable asynchronous transport contract."""

    async def request(self, method: str, url: str, **kwargs: Any) -> AsyncResponse:
        """Send one asynchronous request."""
        ...

    async def aclose(self) -> None:
        """Close the transport."""
        ...


class AsyncPermutiveClient:
    """Strictly typed asynchronous Permutive API client."""

    def __init__(
        self,
        api_key: Optional[Union[str, Secret]] = None,
        *,
        config: Optional[PermutiveConfig] = None,
        base_url: str = "https://api.permutive.com",
        timeout: Tuple[float, float] = (3.05, 30.0),
        retry_policy: Optional[RetryPolicy] = None,
        transport: Optional[AsyncTransport] = None,
    ) -> None:
        if config is None:
            if api_key is None:
                raise ValueError("api_key or config is required")
            secret = api_key if isinstance(api_key, Secret) else Secret(api_key)
            config = PermutiveConfig(
                api_key=secret,
                base_url=base_url,
                timeout=timeout,
                retry_policy=retry_policy or RetryPolicy(),
            )
        self._config = config
        self._transport = transport or self._default_transport(config)

    @staticmethod
    def _default_transport(config: PermutiveConfig) -> AsyncTransport:
        try:
            import httpx
        except ImportError as exc:
            raise RuntimeError(
                "Async support requires `pip install PermutiveAPI[async]` "
                "or an injected AsyncTransport"
            ) from exc
        timeout = httpx.Timeout(config.timeout[1], connect=config.timeout[0])
        return cast(AsyncTransport, httpx.AsyncClient(timeout=timeout))

    async def __aenter__(self) -> "AsyncPermutiveClient":
        """Return the active asynchronous client."""
        return self

    async def __aexit__(self, *_: object) -> None:
        """Close the active asynchronous client."""
        await self.close()

    async def close(self) -> None:
        """Close the underlying transport."""
        await self._transport.aclose()

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Mapping[str, JSONScalar]] = None,
        json: Optional[JSONObject] = None,
        idempotent: Optional[bool] = None,
    ) -> JSONObject:
        """Send one request with bounded retry behavior."""
        verb = method.upper()
        safe = verb in {"GET", "HEAD", "OPTIONS", "DELETE"}
        may_retry = safe if idempotent is None else idempotent
        endpoint = f"{self._config.base_url.rstrip('/')}/{path.lstrip('/')}"
        query: Dict[str, JSONScalar] = dict(params or {})
        query["k"] = self._config.api_key.value
        delay = self._config.retry_policy.initial_delay

        for attempt in range(1, self._config.retry_policy.max_attempts + 1):
            response = await self._transport.request(
                verb,
                endpoint,
                params=query,
                json=json,
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                },
                timeout=self._config.timeout,
            )
            if 200 <= response.status_code < 300:
                payload = response.json()
                if payload is None:
                    return {}
                if not isinstance(payload, dict):
                    raise DecodingError("Permutive returned a non-object JSON payload")
                return cast(JSONObject, payload)

            retryable = response.status_code in self._config.retry_policy.retry_statuses
            if (
                not may_retry
                or not retryable
                or attempt == self._config.retry_policy.max_attempts
            ):
                raise SDKError(
                    f"Permutive request failed with HTTP {response.status_code}",
                    status_code=response.status_code,
                    endpoint=endpoint,
                    retryable=retryable,
                    attempts=attempt,
                )
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                try:
                    delay = min(
                        float(retry_after), self._config.retry_policy.max_delay
                    )
                except ValueError:
                    pass
            await asyncio.sleep(delay)
            delay = min(
                delay * self._config.retry_policy.multiplier,
                self._config.retry_policy.max_delay,
            )
        raise SDKError("Request terminated unexpectedly", endpoint=endpoint)

    async def list_page(
        self,
        path: str,
        *,
        item_decoder: Callable[[JSONObject], T],
        page_size: int = 100,
        continuation: Optional[str] = None,
    ) -> Page[T]:
        """Fetch one typed asynchronous page."""
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
        """Iterate pages lazily with repeated-token protection."""
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


class AsyncResource(Generic[T]):
    """Canonical asynchronous resource facade."""

    def __init__(
        self,
        client: AsyncPermutiveClient,
        path: str,
        decoder: Callable[[JSONObject], T],
    ) -> None:
        self.client = client
        self.path = path
        self.decoder = decoder

    async def get(self, resource_id: str) -> T:
        """Return one resource."""
        payload = await self.client.request("GET", f"{self.path}/{resource_id}")
        return self.decoder(payload)

    async def create(self, payload: JSONObject) -> T:
        """Create one resource."""
        return self.decoder(await self.client.request("POST", self.path, json=payload))

    async def update(self, resource_id: str, payload: JSONObject) -> T:
        """Update one resource."""
        result = await self.client.request(
            "PATCH", f"{self.path}/{resource_id}", json=payload
        )
        return self.decoder(result)

    async def delete(self, resource_id: str) -> None:
        """Delete one resource."""
        await self.client.request("DELETE", f"{self.path}/{resource_id}")
