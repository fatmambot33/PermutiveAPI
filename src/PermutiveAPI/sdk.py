"""Typed client primitives for the stable PermutiveAPI SDK."""

from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Generic, Iterator, List, Mapping, Optional, Protocol, Sequence, Tuple, TypeVar, Union, cast

import requests
from requests import Response, Session

JSONScalar = Union[str, int, float, bool, None]
JSONValue = Union[JSONScalar, List["JSONValue"], Dict[str, "JSONValue"]]
JSONObject = Dict[str, JSONValue]
T = TypeVar("T")
R = TypeVar("R")


class Transport(Protocol):
    """Minimal injectable transport contract."""

    def request(self, method: str, url: str, **kwargs: Any) -> Response:
        """Send one HTTP request."""


@dataclass(frozen=True)
class RetryPolicy:
    """Bounded retry configuration."""

    max_attempts: int = 3
    initial_delay: float = 0.5
    multiplier: float = 2.0
    max_delay: float = 8.0
    jitter: float = 0.1
    retry_statuses: frozenset[int] = field(
        default_factory=lambda: frozenset({429, 500, 502, 503, 504})
    )

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if min(self.initial_delay, self.multiplier, self.max_delay) <= 0:
            raise ValueError("retry delays and multiplier must be positive")
        if self.jitter < 0:
            raise ValueError("jitter must be non-negative")


class SDKError(Exception):
    """Base error for stable SDK operations."""

    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        request_id: Optional[str] = None,
        endpoint: Optional[str] = None,
        retryable: bool = False,
        attempts: int = 1,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.request_id = request_id
        self.endpoint = endpoint
        self.retryable = retryable
        self.attempts = attempts


class AuthenticationError(SDKError):
    """Authentication failed."""


class AuthorizationError(SDKError):
    """Authorization failed."""


class ValidationError(SDKError):
    """Request validation failed."""


class NotFoundError(SDKError):
    """Requested resource was not found."""


class ConflictError(SDKError):
    """Request conflicted with current state."""


class RateLimitError(SDKError):
    """Rate limit was exceeded."""


class ServerError(SDKError):
    """Remote service failed."""


class TransportError(SDKError):
    """Transport failed before a valid response was received."""


class DecodingError(SDKError):
    """Response payload could not be decoded."""


@dataclass(frozen=True)
class Page(Generic[T]):
    """One typed page of API results."""

    items: Tuple[T, ...]
    next_token: Optional[str] = None


@dataclass(frozen=True)
class BatchItem(Generic[T, R]):
    """One batch item outcome."""

    input: T
    value: Optional[R] = None
    error: Optional[Exception] = None

    @property
    def succeeded(self) -> bool:
        return self.error is None


@dataclass(frozen=True)
class BatchResult(Generic[T, R]):
    """Ordered outcomes from bounded batch execution."""

    items: Tuple[BatchItem[T, R], ...]

    @property
    def successes(self) -> Tuple[BatchItem[T, R], ...]:
        return tuple(item for item in self.items if item.succeeded)

    @property
    def failures(self) -> Tuple[BatchItem[T, R], ...]:
        return tuple(item for item in self.items if not item.succeeded)


def _redact(text: str, secret: str) -> str:
    return text.replace(secret, "[REDACTED]") if secret else text


def _error_for_response(response: Response, endpoint: str, attempts: int) -> SDKError:
    status = response.status_code
    request_id = response.headers.get("X-Request-ID") or response.headers.get("Request-ID")
    message = f"Permutive request failed with HTTP {status}"
    kwargs = dict(
        status_code=status,
        request_id=request_id,
        endpoint=endpoint,
        retryable=status == 429 or status >= 500,
        attempts=attempts,
    )
    if status == 401:
        return AuthenticationError(message, **kwargs)
    if status == 403:
        return AuthorizationError(message, **kwargs)
    if status in {400, 422}:
        return ValidationError(message, **kwargs)
    if status == 404:
        return NotFoundError(message, **kwargs)
    if status == 409:
        return ConflictError(message, **kwargs)
    if status == 429:
        return RateLimitError(message, **kwargs)
    if status >= 500:
        return ServerError(message, **kwargs)
    return SDKError(message, **kwargs)


class PermutiveClient:
    """Explicit, synchronous, dependency-injectable Permutive API client."""

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = "https://api.permutive.com",
        timeout: Tuple[float, float] = (3.05, 30.0),
        retry_policy: Optional[RetryPolicy] = None,
        transport: Optional[Transport] = None,
    ) -> None:
        if not api_key:
            raise ValueError("api_key must not be empty")
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._retry = retry_policy or RetryPolicy()
        self._transport = transport or requests.Session()

    def close(self) -> None:
        """Close the underlying session when supported."""
        close = getattr(self._transport, "close", None)
        if callable(close):
            close()

    def __enter__(self) -> "PermutiveClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Mapping[str, JSONScalar]] = None,
        json: Optional[JSONObject] = None,
        idempotent: Optional[bool] = None,
    ) -> JSONObject:
        """Send a request and return a decoded JSON object."""
        verb = method.upper()
        safe = verb in {"GET", "HEAD", "OPTIONS", "DELETE"}
        may_retry = safe if idempotent is None else idempotent
        endpoint = f"{self._base_url}/{path.lstrip('/')}"
        query = dict(params or {})
        query["k"] = self._api_key
        delay = self._retry.initial_delay
        response: Optional[Response] = None

        for attempt in range(1, self._retry.max_attempts + 1):
            try:
                response = self._transport.request(
                    verb,
                    endpoint,
                    params=query,
                    json=json,
                    headers={"Accept": "application/json", "Content-Type": "application/json"},
                    timeout=self._timeout,
                )
            except requests.RequestException as exc:
                if not may_retry or attempt == self._retry.max_attempts:
                    raise TransportError(
                        _redact(str(exc), self._api_key),
                        endpoint=endpoint,
                        retryable=may_retry,
                        attempts=attempt,
                    ) from exc
                self._sleep(delay)
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
                raise _error_for_response(response, endpoint, attempt)
            retry_after = response.headers.get("Retry-After")
            if retry_after is not None:
                try:
                    delay = min(float(retry_after), self._retry.max_delay)
                except ValueError:
                    pass
            self._sleep(delay)
            delay = min(delay * self._retry.multiplier, self._retry.max_delay)

        raise TransportError("Request terminated unexpectedly", endpoint=endpoint)

    def list_page(
        self,
        path: str,
        *,
        item_decoder: Callable[[JSONObject], T],
        page_size: int = 100,
        continuation: Optional[str] = None,
    ) -> Page[T]:
        """Fetch one typed page using shared continuation semantics."""
        if page_size < 1:
            raise ValueError("page_size must be positive")
        params: Dict[str, JSONScalar] = {"limit": page_size}
        if continuation:
            params["continuation"] = continuation
        payload = self.request("GET", path, params=params)
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

    def iter_all(
        self,
        path: str,
        *,
        item_decoder: Callable[[JSONObject], T],
        page_size: int = 100,
        max_items: Optional[int] = None,
    ) -> Iterator[T]:
        """Lazily iterate pages with repeated-token protection."""
        token: Optional[str] = None
        seen: set[str] = set()
        yielded = 0
        while True:
            page = self.list_page(
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

    def _sleep(self, delay: float) -> None:
        jitter = random.uniform(0.0, self._retry.jitter) if self._retry.jitter else 0.0
        time.sleep(min(delay + jitter, self._retry.max_delay))


def execute_batch(
    inputs: Sequence[T],
    operation: Callable[[T], R],
    *,
    max_workers: int = 4,
    fail_fast: bool = False,
    progress: Optional[Callable[[int, int], None]] = None,
) -> BatchResult[T, R]:
    """Execute ordered work with bounded concurrency and typed outcomes."""
    if max_workers < 1:
        raise ValueError("max_workers must be positive")
    if not inputs:
        return BatchResult(items=())

    from concurrent.futures import ThreadPoolExecutor, as_completed

    outcomes: List[Optional[BatchItem[T, R]]] = [None] * len(inputs)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(operation, value): index for index, value in enumerate(inputs)}
        completed = 0
        for future in as_completed(futures):
            index = futures[future]
            value = inputs[index]
            try:
                outcomes[index] = BatchItem(input=value, value=future.result())
            except Exception as exc:  # noqa: BLE001
                outcomes[index] = BatchItem(input=value, error=exc)
                if fail_fast:
                    for pending in futures:
                        pending.cancel()
                    raise
            completed += 1
            if progress is not None:
                progress(completed, len(inputs))
    return BatchResult(items=tuple(item for item in outcomes if item is not None))
