"""HTTP utilities and exceptions used across the package.

This module provides retry-aware HTTP helpers, reusable thread-local sessions,
batch execution, payload conversion, redaction, and purpose-specific errors.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import requests
from requests import Session
from requests.exceptions import RequestException
from requests.models import Response

from .json import to_payload as _json_to_payload


class PermutiveAPIError(Exception):
    """Represent a base exception for PermutiveAPI errors."""

    def __init__(
        self,
        message: str,
        *,
        status: Optional[int] = None,
        url: Optional[str] = None,
        response: Optional[Response] = None,
    ) -> None:
        """Initialise the exception with optional HTTP context."""
        super().__init__(message)
        self.message = message
        self.status = status
        self.url = url
        self.response = response


class PermutiveAuthenticationError(PermutiveAPIError):
    """Represent an authentication failure (HTTP 401 or 403)."""


class PermutiveBadRequestError(PermutiveAPIError):
    """Represent a client-side bad request error (HTTP 400)."""


class PermutiveResourceNotFoundError(PermutiveAPIError):
    """Represent a missing resource error (HTTP 404)."""


class PermutiveRateLimitError(PermutiveAPIError):
    """Represent an API rate limit error (HTTP 429)."""


class PermutiveServerError(PermutiveAPIError):
    """Represent a server-side API error (HTTP 5xx)."""


DEFAULT_HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}
SENSITIVE_QUERY_PARAMS = ("k", "api_key", "token", "access_token", "key")
SUCCESS_RANGE = range(200, 300)
SUPPORTED_METHODS = frozenset({"GET", "POST", "PATCH", "DELETE"})

MAX_RETRIES = 3
BACKOFF_FACTOR = 2.0
INITIAL_DELAY = 1.0

BATCH_MAX_WORKERS_ENV_VAR = "PERMUTIVE_BATCH_MAX_WORKERS"
BATCH_TIMEOUT_ENV_VAR = "PERMUTIVE_BATCH_TIMEOUT_SECONDS"
DEFAULT_BATCH_TIMEOUT = 10.0

RETRY_MAX_RETRIES_ENV_VAR = "PERMUTIVE_RETRY_MAX_RETRIES"
RETRY_BACKOFF_FACTOR_ENV_VAR = "PERMUTIVE_RETRY_BACKOFF_FACTOR"
RETRY_INITIAL_DELAY_ENV_VAR = "PERMUTIVE_RETRY_INITIAL_DELAY_SECONDS"

_THREAD_LOCAL = threading.local()

_REDACTION_PATTERNS = {
    key: (
        re.compile(rf"({re.escape(key)})=([^\s&]+)", flags=re.IGNORECASE),
        re.compile(rf'("{re.escape(key)}"\s*:\s*")[^"]+(\")', flags=re.IGNORECASE),
    )
    for key in SENSITIVE_QUERY_PARAMS
}


def _create_session() -> Session:
    """Create a configured reusable HTTP session.

    Returns
    -------
    requests.Session
        Session with package default headers applied.
    """
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    return session


def get_session() -> Session:
    """Return the reusable HTTP session for the current thread.

    Returns
    -------
    requests.Session
        A session isolated to and reused by the current thread.
    """
    session = getattr(_THREAD_LOCAL, "session", None)
    if session is None:
        session = _create_session()
        _THREAD_LOCAL.session = session
    return session


def close_session() -> None:
    """Close and remove the HTTP session for the current thread."""
    session = getattr(_THREAD_LOCAL, "session", None)
    if session is not None:
        session.close()
        delattr(_THREAD_LOCAL, "session")


def _normalise_env_value(name: str) -> Optional[str]:
    """Return a stripped environment variable value or ``None``."""
    value = os.getenv(name)
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _parse_positive_int(value: str, *, env_var: str) -> int:
    """Return ``value`` parsed as a strictly positive integer."""
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(
            f"Environment variable {env_var} must be a positive integer, got {value!r}."
        ) from exc
    if parsed <= 0:
        raise ValueError(
            f"Environment variable {env_var} must be a positive integer, got {value!r}."
        )
    return parsed


def _parse_positive_float(value: str, *, env_var: str) -> float:
    """Return ``value`` parsed as a strictly positive float."""
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ValueError(
            f"Environment variable {env_var} must be a positive float, got {value!r}."
        ) from exc
    if parsed <= 0:
        raise ValueError(
            f"Environment variable {env_var} must be a positive float, got {value!r}."
        )
    return parsed


def _default_batch_timeout() -> Optional[float]:
    """Resolve the default timeout for batch requests."""
    value = _normalise_env_value(BATCH_TIMEOUT_ENV_VAR)
    if value is None:
        return DEFAULT_BATCH_TIMEOUT
    return _parse_positive_float(value, env_var=BATCH_TIMEOUT_ENV_VAR)


def _resolve_max_workers(max_workers: Optional[int]) -> Optional[int]:
    """Return the executor worker count honoring environment overrides."""
    if max_workers is not None:
        return max_workers
    value = _normalise_env_value(BATCH_MAX_WORKERS_ENV_VAR)
    if value is None:
        return None
    return _parse_positive_int(value, env_var=BATCH_MAX_WORKERS_ENV_VAR)


def _default_max_retries() -> int:
    """Return the default retry attempt count."""
    value = _normalise_env_value(RETRY_MAX_RETRIES_ENV_VAR)
    if value is None:
        return MAX_RETRIES
    return _parse_positive_int(value, env_var=RETRY_MAX_RETRIES_ENV_VAR)


def _default_backoff_factor() -> float:
    """Return the default retry backoff multiplier."""
    value = _normalise_env_value(RETRY_BACKOFF_FACTOR_ENV_VAR)
    if value is None:
        return BACKOFF_FACTOR
    return _parse_positive_float(value, env_var=RETRY_BACKOFF_FACTOR_ENV_VAR)


def _default_initial_delay() -> float:
    """Return the default initial retry delay."""
    value = _normalise_env_value(RETRY_INITIAL_DELAY_ENV_VAR)
    if value is None:
        return INITIAL_DELAY
    return _parse_positive_float(value, env_var=RETRY_INITIAL_DELAY_ENV_VAR)


@dataclass
class RetryConfig:
    """Describe retry and backoff configuration."""

    max_retries: int = field(default_factory=_default_max_retries)
    backoff_factor: float = field(default_factory=_default_backoff_factor)
    initial_delay: float = field(default_factory=_default_initial_delay)


@dataclass
class BatchRequest:
    """Describe an HTTP request executed within a batch."""

    method: str
    url: str
    params: Optional[Dict[str, Any]] = None
    json: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, str]] = None
    timeout: Optional[float] = field(default_factory=_default_batch_timeout)
    retry: Optional[RetryConfig] = None
    callback: Optional[Callable[[Response], None]] = None
    error_callback: Optional[Callable[[Exception], None]] = None


@dataclass(frozen=True)
class Progress:
    """Describe an immutable snapshot of batch processing progress."""

    completed: int
    total: int
    errors: int
    batch_request: BatchRequest
    elapsed_seconds: float
    average_per_thousand_seconds: Optional[float]


def get(
    api_key: str,
    url: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    session: Optional[Session] = None,
) -> Response:
    """Perform a GET request with retry logic."""
    active_session = session or get_session()
    return _with_retry(active_session.get, url, api_key, params=params)


def post(
    api_key: str,
    url: str,
    data: dict,
    *,
    session: Optional[Session] = None,
) -> Response:
    """Perform a POST request with retry logic."""
    active_session = session or get_session()
    return _with_retry(active_session.post, url, api_key, json=data)


def patch(
    api_key: str,
    url: str,
    data: dict,
    *,
    session: Optional[Session] = None,
) -> Response:
    """Perform a PATCH request with retry logic."""
    active_session = session or get_session()
    return _with_retry(active_session.patch, url, api_key, json=data)


def delete(
    api_key: str,
    url: str,
    *,
    session: Optional[Session] = None,
) -> Response:
    """Perform a DELETE request with retry logic."""
    active_session = session or get_session()
    return _with_retry(active_session.delete, url, api_key)


def request(
    method: str,
    api_key: str,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    json: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: Optional[float] = 10.0,
    retry: Optional[RetryConfig] = None,
    session: Optional[Session] = None,
) -> Response:
    """Perform an HTTP request through a reusable session."""
    normalised_method = method.upper()
    if normalised_method not in SUPPORTED_METHODS:
        raise ValueError(f"Unsupported HTTP method: {method}")

    merged_headers = dict(DEFAULT_HEADERS)
    if headers:
        merged_headers.update(headers)

    kwargs: Dict[str, Any] = {}
    if params:
        kwargs["params"] = params
    if json is not None:
        kwargs["json"] = json
    if timeout is not None:
        kwargs["timeout"] = timeout

    active_session = session or get_session()

    def send(target_url: str, **request_kwargs: Any) -> Response:
        return active_session.request(
            method=normalised_method,
            url=target_url,
            **request_kwargs,
        )

    return _with_retry(
        send,
        url,
        api_key,
        headers=merged_headers,
        retry=retry,
        **kwargs,
    )


def to_payload(
    dataclass_obj: Any, api_payload: Optional[List[str]] = None
) -> Dict[str, Any]:
    """Convert a dataclass object to a JSON-compatible payload."""
    return _json_to_payload(dataclass_obj, api_payload)


def _with_retry(
    method: Callable[..., Response],
    url: str,
    api_key: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    retry: Optional[RetryConfig] = None,
    **kwargs: Any,
) -> Response:
    """Execute an HTTP callable with retry handling."""
    resolved_retry = retry or RetryConfig()
    params = (kwargs.pop("params", {}) or {}).copy()
    params["k"] = api_key
    kwargs["params"] = params

    attempt = 0
    delay = resolved_retry.initial_delay
    response: Optional[Response] = None
    last_exception: Optional[RequestException] = None

    while attempt < resolved_retry.max_retries:
        response = None
        try:
            merged_headers = headers if headers is not None else DEFAULT_HEADERS
            response = method(url, headers=merged_headers, **kwargs)
            if response.status_code in SUCCESS_RANGE:
                return response

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", delay))
                logging.warning(
                    "429 Too Many Requests: retrying in %ss (attempt %s)",
                    retry_after,
                    attempt + 1,
                )
                time.sleep(retry_after)
            elif 500 <= response.status_code < 600:
                logging.warning(
                    "%s Server Error: retrying in %ss (attempt %s)",
                    response.status_code,
                    delay,
                    attempt + 1,
                )
                time.sleep(delay)
                delay *= resolved_retry.backoff_factor
            else:
                raise_for_status(
                    RequestException(f"HTTP {response.status_code}"), response
                )

        except RequestException as exc:
            last_exception = exc
            if attempt >= resolved_retry.max_retries - 1:
                redacted_error = redact_message(str(exc))
                logging.error(
                    "Request failed after %s attempts: %s",
                    attempt + 1,
                    redacted_error,
                )
                if response is not None:
                    try:
                        raise_for_status(RequestException(redacted_error), response)
                    except PermutiveAPIError as api_error:
                        raise api_error from exc
                raise PermutiveAPIError(
                    f"Request failed after {attempt + 1} attempts: {redacted_error}"
                ) from exc
            time.sleep(delay)
            delay *= resolved_retry.backoff_factor

        attempt += 1

    final_exception = last_exception or RequestException("Max retries reached")
    redacted_message = redact_message(str(final_exception))
    if response is not None:
        try:
            raise_for_status(RequestException(redacted_message), response)
        except PermutiveAPIError as api_error:
            raise api_error from final_exception

    logging.error(
        "Request failed after %s attempts: %s",
        resolved_retry.max_retries,
        redacted_message,
    )
    raise PermutiveAPIError(
        f"Request failed after {resolved_retry.max_retries} attempts: {redacted_message}"
    ) from final_exception


def process_batch(
    requests: Iterable[BatchRequest],
    *,
    api_key: str,
    max_workers: Optional[int],
    progress_callback: Optional[Callable[[Progress], None]] = None,
) -> Tuple[List[Response], List[Tuple[BatchRequest, Exception]]]:
    """Execute multiple HTTP requests concurrently."""
    batch_requests = list(requests)
    total = len(batch_requests)
    if total == 0:
        return [], []

    responses: List[Response] = []
    errors: List[Tuple[BatchRequest, Exception]] = []
    start_time = time.perf_counter()
    resolved_max_workers = _resolve_max_workers(max_workers)

    with ThreadPoolExecutor(max_workers=resolved_max_workers) as executor:
        future_to_request = {
            executor.submit(
                request,
                batch_request.method,
                api_key,
                batch_request.url,
                params=batch_request.params,
                json=batch_request.json,
                headers=batch_request.headers,
                timeout=batch_request.timeout,
                retry=batch_request.retry,
            ): batch_request
            for batch_request in batch_requests
        }

        completed = 0
        for future in as_completed(future_to_request):
            batch_request = future_to_request[future]
            try:
                response = future.result()
            except Exception as exc:  # noqa: BLE001
                errors.append((batch_request, exc))
                if batch_request.error_callback is not None:
                    try:
                        batch_request.error_callback(exc)
                    except Exception:
                        logging.exception("Error callback raised an exception")
            else:
                responses.append(response)
                if batch_request.callback is not None:
                    try:
                        batch_request.callback(response)
                    except Exception:
                        logging.exception("Success callback raised an exception")
            finally:
                completed += 1
                if progress_callback is not None:
                    try:
                        elapsed = time.perf_counter() - start_time
                        average_per_thousand = (elapsed / completed) * 1000
                        progress_callback(
                            Progress(
                                completed=completed,
                                total=total,
                                errors=len(errors),
                                batch_request=batch_request,
                                elapsed_seconds=elapsed,
                                average_per_thousand_seconds=average_per_thousand,
                            )
                        )
                    except Exception:
                        logging.exception("Progress callback raised an exception")

    return responses, errors


def redact_message(message: str) -> str:
    """Redact sensitive tokens in free-form text and JSON snippets."""
    for key in SENSITIVE_QUERY_PARAMS:
        param_pattern, json_pattern = _REDACTION_PATTERNS[key]
        message = param_pattern.sub(rf"\1=[REDACTED]", message)
        message = json_pattern.sub(rf"\1[REDACTED]\2", message)
    return message


def redact_url(url: str) -> str:
    """Return a URL with sensitive query values redacted."""
    try:
        parsed = urllib.parse.urlparse(url)
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        sensitive = {key.lower() for key in SENSITIVE_QUERY_PARAMS}
        for key in list(query.keys()):
            if key.lower() in sensitive:
                query[key] = ["[REDACTED]" for _ in query[key]]
        redacted_query = urllib.parse.urlencode(query, doseq=True)
        return urllib.parse.urlunparse(parsed._replace(query=redacted_query))
    except Exception:
        return url


def _redact_sensitive_data(message: str, response: Response) -> str:
    """Redact sensitive data from a message and response request URL."""
    if hasattr(response, "request") and hasattr(response.request, "url"):
        parsed_url = urllib.parse.urlparse(response.request.url)
        query_params = urllib.parse.parse_qs(str(parsed_url.query))
        for key in SENSITIVE_QUERY_PARAMS:
            if key in query_params:
                for secret in query_params[key]:
                    if secret:
                        message = message.replace(secret, "[REDACTED]")
    return redact_message(message)


def _extract_error_message(response: Response) -> str:
    """Extract a human-readable API error message."""
    try:
        error_content = json.loads(response.content)
        if isinstance(error_content, dict):
            error_details = error_content.get("error")
            if isinstance(error_details, dict):
                return str(error_details.get("cause", "Unknown error"))
    except Exception:
        return "Could not parse error message"
    return "Unknown error"


def raise_for_status(error: Exception, response: Optional[Response]) -> None:
    """Raise a custom exception based on an HTTP response status."""
    if response is not None:
        status = response.status_code
        if status in SUCCESS_RANGE:
            return
        redacted_url = None
        request_obj = getattr(response, "request", None)
        request_url = getattr(request_obj, "url", None)
        if request_url:
            redacted_url = redact_url(request_url)

        if status == 400:
            message = redact_message(_extract_error_message(response))
            display_url = urllib.parse.unquote(redacted_url) if redacted_url else None
            full_message = f"400 Bad Request: {message}" + (
                f" [URL: {display_url}]" if display_url else ""
            )
            raise PermutiveBadRequestError(
                full_message,
                status=status,
                url=redacted_url,
                response=response,
            ) from error
        if status in (401, 403):
            raise PermutiveAuthenticationError(
                f"{status}: Invalid API key or insufficient permissions.",
                status=status,
                url=redacted_url,
                response=response,
            ) from error
        if status == 404:
            raise PermutiveResourceNotFoundError(
                "Resource not found.",
                status=status,
                url=redacted_url,
                response=response,
            ) from error
        if status == 429:
            raise PermutiveRateLimitError(
                "Retry limit exceeded.",
                status=status,
                url=redacted_url,
                response=response,
            ) from error
        if 500 <= status < 600:
            raise PermutiveServerError(
                f"{status}: API unavailable after retries.",
                status=status,
                url=redacted_url,
                response=response,
            ) from error

    raise PermutiveAPIError(f"An unexpected error occurred: {error}") from error


__all__ = [
    "request",
    "process_batch",
    "get",
    "post",
    "patch",
    "delete",
    "get_session",
    "close_session",
    "to_payload",
    "redact_url",
    "redact_message",
    "RetryConfig",
    "BatchRequest",
    "Progress",
    "raise_for_status",
    "PermutiveAPIError",
    "PermutiveAuthenticationError",
    "PermutiveBadRequestError",
    "PermutiveResourceNotFoundError",
    "PermutiveRateLimitError",
    "PermutiveServerError",
]
