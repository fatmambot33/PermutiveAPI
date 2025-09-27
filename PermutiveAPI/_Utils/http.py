"""HTTP utilities and exceptions used across the package.

This module provides:
- Lightweight HTTP helpers (get, post, patch, delete) with retry logic,
  redaction, and consistent error handling.
- ``BatchRequest``/``process_batch`` utilities for concurrent operations
  powered by :class:`~concurrent.futures.ThreadPoolExecutor`. The default
  worker limit follows the standard Python behaviour (``min(32, os.cpu_count()
  + 4)``) when ``max_workers`` is ``None``. Helpers only rely on stateless
  module-level functions, so they are safe to call concurrently provided that
  user-supplied callbacks are thread-safe.
- to_payload helper for dataclass-to-JSON conversion using the custom encoder.
- Exception classes representing API error conditions.
"""

from __future__ import annotations
import json
import logging
import re
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import requests
from requests.exceptions import RequestException
from requests.models import Response

from .json import customJSONEncoder


class PermutiveAPIError(Exception):
    """Base exception class for all PermutiveAPI errors.

    Parameters
    ----------
    message : str
        Human-friendly error message.
    status : int | None, optional
        HTTP status code associated with the error.
    url : str | None, optional
        Redacted request URL, when available.
    response : requests.Response | None, optional
        Original response object for debugging.
    """

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
    """Raised when authentication fails (HTTP 401 or 403)."""

    pass


class PermutiveBadRequestError(PermutiveAPIError):
    """Raised for client-side errors (HTTP 400)."""

    def __init__(
        self,
        message: str,
        *args,
        status: Optional[int] = None,
        url: Optional[str] = None,
        response: Optional[Response] = None,
        **kwargs,
    ) -> None:
        """Initialise the bad request error with optional HTTP context."""
        super().__init__(message, status=status, url=url, response=response)


class PermutiveResourceNotFoundError(PermutiveAPIError):
    """Raised when a requested resource is not found (HTTP 404)."""

    pass


class PermutiveRateLimitError(PermutiveAPIError):
    """Raised when the API rate limit is exceeded (HTTP 429)."""

    pass


class PermutiveServerError(PermutiveAPIError):
    """Raised for server-side errors (HTTP 5xx)."""

    pass


DEFAULT_HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}
SENSITIVE_QUERY_PARAMS = ("k", "api_key", "token", "access_token", "key")
SUCCESS_RANGE = range(200, 300)

MAX_RETRIES = 3
BACKOFF_FACTOR = 2
INITIAL_DELAY = 1


@dataclass
class RetryConfig:
    """Configuration for retry/backoff behaviour.

    Parameters
    ----------
    max_retries : int
        Maximum retry attempts. Defaults to 3.
    backoff_factor : int
        Exponential backoff multiplier. Defaults to 2.
    initial_delay : int
        Initial delay in seconds before the first retry. Defaults to 1.
    """

    max_retries: int = MAX_RETRIES
    backoff_factor: int = BACKOFF_FACTOR
    initial_delay: int = INITIAL_DELAY


@dataclass
class BatchRequest:
    """Container describing an HTTP request executed within a batch.

    Methods
    -------
    None
        Instances provide dataclass-generated attribute access only.

    Parameters
    ----------
    method : str
        HTTP method name (GET, POST, PATCH, DELETE).
    url : str
        Absolute URL to target.
    params : dict | None, optional
        Query parameters to include. Defaults to ``None``.
    json : dict | None, optional
        JSON payload for methods that support it. Defaults to ``None``.
    headers : dict | None, optional
        Extra headers merged with :data:`DEFAULT_HEADERS`. Defaults to ``None``.
    timeout : float | None, optional
        Requests timeout in seconds. Defaults to ``10.0``.
    retry : RetryConfig | None, optional
        Override retry configuration for the request. Defaults to ``None``.
    callback : Callable[[requests.Response], None] | None, optional
        Invoked with the successful response. Defaults to ``None``.
    error_callback : Callable[[Exception], None] | None, optional
        Invoked with the raised exception if the request fails. Defaults to
        ``None``.
    """

    method: str
    url: str
    params: Optional[Dict[str, Any]] = None
    json: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, str]] = None
    timeout: Optional[float] = 10.0
    retry: Optional[RetryConfig] = None
    callback: Optional[Callable[[Response], None]] = None
    error_callback: Optional[Callable[[Exception], None]] = None


@dataclass(frozen=True)
class Progress:
    """Immutable snapshot describing batch processing progress.

    Parameters
    ----------
    completed : int
        Number of requests that have finished (successfully or with an
        exception).
    total : int
        Total number of requests in the batch.
    errors : int
        Number of requests that have failed so far.
    batch_request : BatchRequest
        Descriptor corresponding to the request that just finished.
    elapsed_seconds : float
        Wall-clock seconds elapsed since :func:`process_batch` started.
    average_per_thousand_seconds : float | None
        Estimated seconds required to process 1,000 requests based on the
        observed throughput. ``None`` when insufficient data is available
        (e.g. before any request has completed).
    """

    completed: int
    total: int
    errors: int
    batch_request: BatchRequest
    elapsed_seconds: float
    average_per_thousand_seconds: Optional[float]


def get(api_key: str, url: str, params: Optional[Dict[str, Any]] = None) -> Response:
    """Perform a GET request with retry logic.

    Parameters
    ----------
    api_key : str
        The API key for authentication.
    url : str
        The URL for the request.
    params : dict, optional
        Query parameters to include in the request.

    Returns
    -------
    requests.Response
        The response object from the API.

    Raises
    ------
    PermutiveAPIError
        If the request fails after all retries.
    """
    return _with_retry(requests.get, url, api_key, params=params)


def post(api_key: str, url: str, data: dict) -> Response:
    """Perform a POST request with retry logic.

    Parameters
    ----------
    api_key : str
        The API key for authentication.
    url : str
        The URL for the request.
    data : dict
        The JSON payload to send with the request.

    Returns
    -------
    requests.Response
        The response object from the API.

    Raises
    ------
    PermutiveAPIError
        If the request fails after all retries.
    """
    return _with_retry(requests.post, url, api_key, json=data)


def patch(api_key: str, url: str, data: dict) -> Response:
    """Perform a PATCH request with retry logic.

    Parameters
    ----------
    api_key : str
        The API key for authentication.
    url : str
        The URL for the request.
    data : dict
        The JSON payload to send with the request.

    Returns
    -------
    requests.Response
        The response object from the API.

    Raises
    ------
    PermutiveAPIError
        If the request fails after all retries.
    """
    return _with_retry(requests.patch, url, api_key, json=data)


def delete(api_key: str, url: str) -> Response:
    """Perform a DELETE request with retry logic.

    Parameters
    ----------
    api_key : str
        The API key for authentication.
    url : str
        The URL for the request.

    Returns
    -------
    requests.Response
        The response object from the API.

    Raises
    ------
    PermutiveAPIError
        If the request fails after all retries.
    """
    return _with_retry(requests.delete, url, api_key)


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
) -> Response:
    """Perform an HTTP request with retry and sensible defaults.

    Parameters
    ----------
    method : str
        HTTP method name (GET, POST, PATCH, DELETE).
    api_key : str
        API key to include as `k` query parameter.
    url : str
        Absolute URL.
    params : dict | None, optional
        Query parameters to include.
    json : dict | None, optional
        JSON body for methods that support it.
    headers : dict | None, optional
        Extra headers to merge with DEFAULT_HEADERS (extra wins).
    timeout : float | None, optional
        Requests timeout in seconds. Defaults to 10.0.
    retry : RetryConfig | None, optional
        Override retry configuration. Defaults to module constants.

    Returns
    -------
    requests.Response
        The response object from the API.

    Raises
    ------
    ValueError
        If an unsupported HTTP method is provided.
    PermutiveAPIError
        If the request fails after all retries.
    """
    session_method_map = {
        "GET": requests.get,
        "POST": requests.post,
        "PATCH": requests.patch,
        "DELETE": requests.delete,
    }
    m = session_method_map.get(method.upper())
    if m is None:
        raise ValueError(f"Unsupported HTTP method: {method}")

    hdrs = dict(DEFAULT_HEADERS)
    if headers:
        hdrs.update(headers)

    kwargs: Dict[str, Any] = {}
    if params:
        kwargs["params"] = params
    if json is not None:
        kwargs["json"] = json
    if timeout is not None:
        kwargs["timeout"] = timeout

    if retry is None or (
        retry.max_retries == MAX_RETRIES
        and retry.backoff_factor == BACKOFF_FACTOR
        and retry.initial_delay == INITIAL_DELAY
    ):
        return _with_retry(m, url, api_key, headers=hdrs, **kwargs)

    params_copy = (kwargs.pop("params", {}) or {}).copy()
    params_copy["k"] = api_key
    kwargs["params"] = params_copy

    attempt = 0
    delay = retry.initial_delay
    response: Optional[Response] = None
    while attempt < retry.max_retries:
        try:
            response = m(url, headers=hdrs, **kwargs)
            assert response is not None
            if response.status_code in SUCCESS_RANGE:
                return response
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", delay))
                logging.warning(
                    f"429 Too Many Requests: retrying in {retry_after}s (attempt {attempt+1})"
                )
                time.sleep(retry_after)
            elif 500 <= response.status_code < 600:
                logging.warning(
                    f"{response.status_code} Server Error: retrying in {delay}s (attempt {attempt+1})"
                )
                time.sleep(delay)
                delay *= retry.backoff_factor
            else:
                raise_for_status(
                    RequestException(f"HTTP {response.status_code}"), response
                )
        except RequestException as e:
            if attempt >= retry.max_retries - 1:
                redacted_error = redact_message(str(e))
                logging.error(
                    "Request failed after %s attempts: %s",
                    attempt + 1,
                    redacted_error,
                )
                raise PermutiveAPIError(
                    f"Request failed after {attempt+1} attempts: {redacted_error}"
                ) from e
            time.sleep(delay)
            delay *= retry.backoff_factor
        attempt += 1

    raise RequestException("Max retries reached")


def to_payload(
    dataclass_obj: Any, api_payload: Optional[List[str]] = None
) -> Dict[str, Any]:
    """Convert a dataclass object to a dictionary payload.

    Fields with ``None`` values are omitted. If ``api_payload`` is provided,
    only keys included in this list are kept.

    Parameters
    ----------
    dataclass_obj : Any
        The dataclass instance to convert.
    api_payload : list[str], optional
        A list of attribute names to include in the payload. If `None`, all
        attributes are considered. Defaults to `None`.

    Returns
    -------
    dict[str, Any]
        A dictionary representing the JSON payload.
    """
    dataclass_dict = vars(dataclass_obj)
    filtered_dict = {
        k: v
        for k, v in dataclass_dict.items()
        if v is not None and (api_payload is None or k in api_payload)
    }
    filtered_dict_string = json.dumps(filtered_dict, indent=4, cls=customJSONEncoder)
    return json.loads(filtered_dict_string)


def _with_retry(
    method: Callable,
    url: str,
    api_key: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> Response:
    """Retry logic for transient errors and rate limiting."""
    params = (kwargs.pop("params", {}) or {}).copy()
    params["k"] = api_key
    kwargs["params"] = params

    attempt = 0
    delay = INITIAL_DELAY
    response: Optional[Response] = None

    while attempt < MAX_RETRIES:
        try:
            merged_headers = headers if headers is not None else DEFAULT_HEADERS
            response = method(url, headers=merged_headers, **kwargs)
            assert response is not None
            if response.status_code in SUCCESS_RANGE:
                return response

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", delay))
                logging.warning(
                    f"429 Too Many Requests: retrying in {retry_after}s (attempt {attempt+1})"
                )
                time.sleep(retry_after)
            elif 500 <= response.status_code < 600:
                logging.warning(
                    f"{response.status_code} Server Error: retrying in {delay}s (attempt {attempt+1})"
                )
                time.sleep(delay)
                delay *= BACKOFF_FACTOR
            else:
                raise_for_status(
                    RequestException(f"HTTP {response.status_code}"), response
                )

        except RequestException as e:
            if attempt >= MAX_RETRIES - 1:
                redacted_error = redact_message(str(e))
                logging.error(
                    "Request failed after %s attempts: %s",
                    attempt + 1,
                    redacted_error,
                )
                raise PermutiveAPIError(
                    f"Request failed after {attempt+1} attempts: {redacted_error}"
                ) from e
            time.sleep(delay)
            delay *= BACKOFF_FACTOR

        attempt += 1

    raise RequestException("Max retries reached")


def process_batch(
    requests: Iterable[BatchRequest],
    *,
    api_key: str,
    max_workers: Optional[int],
    progress_callback: Optional[Callable[[Progress], None]] = None,
) -> Tuple[List[Response], List[Tuple[BatchRequest, Exception]]]:
    """Execute multiple HTTP requests concurrently.

    Parameters
    ----------
    requests : Iterable[BatchRequest]
        Sequence of :class:`BatchRequest` descriptors.
    api_key : str
        API key appended to each request.
    max_workers : int | None
        Maximum number of worker threads. When ``None`` the executor applies
        its default limit (``min(32, os.cpu_count() + 4)``).
    progress_callback : Callable[[Progress], None] | None, optional
        Invoked after each request completes with a
        :class:`~PermutiveAPI._Utils.http.Progress` snapshot containing the
        aggregate metrics recorded so far. Defaults to ``None``.

    Returns
    -------
    responses : list[requests.Response]
        Successful responses in the order they complete.
    errors : list[tuple[BatchRequest, Exception]]
        Collected failures paired with their originating request.
    """
    batch_requests = list(requests)
    total = len(batch_requests)
    if total == 0:
        return [], []

    responses: List[Response] = []
    errors: List[Tuple[BatchRequest, Exception]] = []

    start_time = time.perf_counter()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
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
                    except Exception:  # pragma: no cover - defensive logging
                        logging.exception("Error callback raised an exception")
            else:
                responses.append(response)
                if batch_request.callback is not None:
                    try:
                        batch_request.callback(response)
                    except Exception:  # pragma: no cover - defensive logging
                        logging.exception("Success callback raised an exception")
            finally:
                completed += 1
                if progress_callback is not None:
                    try:
                        elapsed = time.perf_counter() - start_time
                        average_per_thousand = (
                            (elapsed / completed) * 1000 if completed else None
                        )
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
                    except Exception:  # pragma: no cover - defensive logging
                        logging.exception("Progress callback raised an exception")

    return responses, errors


def redact_message(message: str) -> str:
    """Redact sensitive tokens in free-form text and JSON snippets.

    Parameters
    ----------
    message : str
        The string containing potentially sensitive information.

    Returns
    -------
    str
        The message with sensitive tokens redacted.
    """
    for key in SENSITIVE_QUERY_PARAMS:
        message = re.sub(
            rf"({key})=([^\s&]+)", rf"\1=[REDACTED]", message, flags=re.IGNORECASE
        )
        message = re.sub(
            rf'("{key}"\s*:\s*")[^"]+(\")',
            rf"\1[REDACTED]\2",
            message,
            flags=re.IGNORECASE,
        )
    return message


def redact_url(url: str) -> str:
    """Return a copy of url with sensitive query parameter values redacted.

    Parameters
    ----------
    url : str
        The URL to redact.

    Returns
    -------
    str
        The redacted URL.
    """
    try:
        parsed = urllib.parse.urlparse(url)
        q = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        sens = {k.lower() for k in SENSITIVE_QUERY_PARAMS}
        for key in list(q.keys()):
            if key.lower() in sens:
                q[key] = ["[REDACTED]" for _ in q[key]]
        redacted_q = urllib.parse.urlencode(q, doseq=True)
        return urllib.parse.urlunparse(parsed._replace(query=redacted_q))
    except Exception:
        return url


def _redact_sensitive_data(message: str, response: Response) -> str:
    """Redact sensitive data from message and request URL query parameters."""
    if hasattr(response, "request") and hasattr(response.request, "url"):
        parsed_url = urllib.parse.urlparse(response.request.url)
        query_params = urllib.parse.parse_qs(str(parsed_url.query))
        for key in SENSITIVE_QUERY_PARAMS:
            if key in query_params:
                for secret in query_params[key]:
                    if secret:
                        message = message.replace(secret, "[REDACTED]")
    for key in SENSITIVE_QUERY_PARAMS:
        message = re.sub(
            rf"({key})=([^\s&]+)", rf"\1=[REDACTED]", message, flags=re.IGNORECASE
        )
        message = re.sub(
            rf'("{key}"\s*:\s*")[^"]+(\")',
            rf"\1[REDACTED]\2",
            message,
            flags=re.IGNORECASE,
        )
    return message


def _extract_error_message(response: Response) -> str:
    try:
        error_content = json.loads(response.content)
        if isinstance(error_content, dict):
            error_details = error_content.get("error")
            if isinstance(error_details, dict):
                return str(error_details.get("cause", "Unknown error"))
    except Exception:
        return "Could not parse error message"
    return "Unknown error"


def raise_for_status(e: Exception, response: Optional[Response]) -> None:
    """Raise a custom exception based on HTTP status, with redaction.

    Parameters
    ----------
    e : Exception
        The original exception that was caught.
    response : requests.Response, optional
        The HTTP response object, if available.

    Raises
    ------
    PermutiveAPIError
        Or one of its subclasses, depending on the response status code.
    """
    if response is not None:
        status = response.status_code
        if status in SUCCESS_RANGE:
            return  # pragma: no cover
        redacted_url = None
        if hasattr(response, "request") and response.request.url:
            redacted_url = redact_url(response.request.url)

        if status == 400:
            msg = redact_message(_extract_error_message(response))
            display_url = urllib.parse.unquote(redacted_url) if redacted_url else None
            full_msg = f"400 Bad Request: {msg}" + (
                f" [URL: {display_url}]" if display_url else ""
            )
            raise PermutiveBadRequestError(
                full_msg, status=status, url=redacted_url, response=response
            ) from e

        if status == 401 or status == 403:
            raise PermutiveAuthenticationError(
                f"{status}: Invalid API key or insufficient permissions.",
                status=status,
                url=redacted_url,
                response=response,
            ) from e

        if status == 404:
            raise PermutiveResourceNotFoundError(
                "Resource not found.",
                status=status,
                url=redacted_url,
                response=response,
            ) from e

        if status == 429:
            raise PermutiveRateLimitError(
                "Retry limit exceeded.",
                status=status,
                url=redacted_url,
                response=response,
            ) from e

        if 500 <= status < 600:
            raise PermutiveServerError(
                f"{status}: API unavailable after retries.",
                status=status,
                url=redacted_url,
                response=response,
            ) from e

    raise PermutiveAPIError(f"An unexpected error occurred: {e}") from e


__all__ = [
    "request",
    "process_batch",
    "get",
    "post",
    "patch",
    "delete",
    "to_payload",
    "redact_url",
    "redact_message",
    "RetryConfig",
    "BatchRequest",
    "raise_for_status",
    "PermutiveAPIError",
    "PermutiveAuthenticationError",
    "PermutiveBadRequestError",
    "PermutiveResourceNotFoundError",
    "PermutiveRateLimitError",
    "PermutiveServerError",
]
