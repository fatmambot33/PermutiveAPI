"""Utility helpers for the Permutive API."""

import re
import requests
from requests.exceptions import RequestException
from requests.models import Response
import json
import pathlib
import ast
from typing import (
    Dict,
    List,
    Optional,
    Any,
    Union,
    Type,
    TypeVar,
    Callable,
    overload,
    Generic,
    Tuple,
    cast,
)
from typing import get_origin, get_args
import logging
import os
import urllib.parse
from dataclasses import is_dataclass, fields
import uuid
from decimal import Decimal
from enum import Enum
import datetime
from pathlib import Path
import time
import sys

from PermutiveAPI.errors import (
    PermutiveAPIError,
    PermutiveAuthenticationError,
    PermutiveBadRequestError,
    PermutiveRateLimitError,
    PermutiveResourceNotFoundError,
    PermutiveServerError,
)


class RequestHelper:
    """A utility class for making HTTP requests to a RESTful API.

    Methods
    -------
    get_static(api_key, url, params=None)
        Perform a GET request with retry logic.
    post_static(api_key, url, data)
        Perform a POST request with retry logic.
    patch_static(api_key, url, data)
        Perform a PATCH request with retry logic.
    delete_static(api_key, url)
        Perform a DELETE request with retry logic.
    get(url)
        Perform a GET request using the instance's API key.
    post(url, data)
        Perform a POST request using the instance's API key.
    patch(url, data)
        Perform a PATCH request using the instance's API key.
    delete(url)
        Perform a DELETE request using the instance's API key.
    to_payload_static(dataclass_obj, api_payload=None)
        Convert a dataclass object to a dictionary payload.
    handle_exception(e, response)
        Handle exceptions and HTTP errors.
    """

    DEFAULT_HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}
    SENSITIVE_QUERY_KEYS = ("k", "api_key", "token", "access_token", "key")
    SUCCESS_RANGE = range(200, 300)

    MAX_RETRIES = 3  # Max retry attempts
    BACKOFF_FACTOR = 2  # Exponential backoff multiplier
    INITIAL_DELAY = 1  # Initial retry delay (seconds)

    api_key: str
    api_endpoint: str
    payload_keys: Optional[List[str]] = None

    def __init__(
        self, api_key: str, api_endpoint: str, payload_keys: Optional[List[str]] = None
    ) -> None:
        """
        Initialise the RequestHelper.

        Parameters
        ----------
        api_key : str
            The API key for authentication.
        api_endpoint : str
            The API endpoint.
        payload_keys : Optional[List[str]], optional
            A list of keys to include in the payload. Defaults to None.
        """
        self.api_key = api_key
        self.api_endpoint = api_endpoint
        self.payload_keys = payload_keys

    # -------- Public Request Methods --------
    @staticmethod
    def get_static(
        api_key: str, url: str, params: Optional[Dict[str, Any]] = None
    ) -> Response:
        """
        Perform a GET request with retry logic.

        Parameters
        ----------
        api_key : str
            The API key for authentication.
        url : str
            The URL to send the request to.
        params : Optional[Dict[str, Any]], optional
            A dictionary of query parameters. Defaults to None.

        Returns
        -------
        requests.Response
            The response from the server.

        Raises
        ------
        PermutiveBadRequestError
            For HTTP 400 errors.
        PermutiveAuthenticationError
            For HTTP 401 or 403 errors.
        PermutiveResourceNotFoundError
            For HTTP 404 errors.
        PermutiveRateLimitError
            For HTTP 429 errors (after retries).
        PermutiveServerError
            For HTTP 5xx errors (after retries).
        PermutiveAPIError
            For other unexpected errors.
        """
        return RequestHelper._with_retry(requests.get, url, api_key, params=params)

    @staticmethod
    def post_static(api_key: str, url: str, data: dict) -> Response:
        """
        Perform a POST request with retry logic.

        Parameters
        ----------
        api_key : str
            The API key for authentication.
        url : str
            The URL to send the request to.
        data : dict
            The data to send in the request body.

        Returns
        -------
        requests.Response
            The response from the server.

        Raises
        ------
        PermutiveBadRequestError
            For HTTP 400 errors.
        PermutiveAuthenticationError
            For HTTP 401 or 403 errors.
        PermutiveResourceNotFoundError
            For HTTP 404 errors.
        PermutiveRateLimitError
            For HTTP 429 errors (after retries).
        PermutiveServerError
            For HTTP 5xx errors (after retries).
        PermutiveAPIError
            For other unexpected errors.
        """
        return RequestHelper._with_retry(requests.post, url, api_key, json=data)

    @staticmethod
    def patch_static(api_key: str, url: str, data: dict) -> Response:
        """
        Perform a PATCH request with retry logic.

        Parameters
        ----------
        api_key : str
            The API key for authentication.
        url : str
            The URL to send the request to.
        data : dict
            The data to send in the request body.

        Returns
        -------
        requests.Response
            The response from the server.

        Raises
        ------
        PermutiveBadRequestError
            For HTTP 400 errors.
        PermutiveAuthenticationError
            For HTTP 401 or 403 errors.
        PermutiveResourceNotFoundError
            For HTTP 404 errors.
        PermutiveRateLimitError
            For HTTP 429 errors (after retries).
        PermutiveServerError
            For HTTP 5xx errors (after retries).
        PermutiveAPIError
            For other unexpected errors.
        """
        return RequestHelper._with_retry(requests.patch, url, api_key, json=data)

    @staticmethod
    def delete_static(api_key: str, url: str) -> Response:
        """
        Perform a DELETE request with retry logic.

        Parameters
        ----------
        api_key : str
            The API key for authentication.
        url : str
            The URL to send the request to.

        Returns
        -------
        requests.Response
            The response from the server.

        Raises
        ------
        PermutiveBadRequestError
            For HTTP 400 errors.
        PermutiveAuthenticationError
            For HTTP 401 or 403 errors.
        PermutiveResourceNotFoundError
            For HTTP 404 errors.
        PermutiveRateLimitError
            For HTTP 429 errors (after retries).
        PermutiveServerError
            For HTTP 5xx errors (after retries).
        PermutiveAPIError
            For other unexpected errors.
        """
        return RequestHelper._with_retry(requests.delete, url, api_key)

    def get(self, url) -> Response:
        """
        Perform a GET request using the instance's API key.

        Parameters
        ----------
        url : str
            The URL to send the request to.

        Returns
        -------
        requests.Response
            The response from the server.

        Raises
        ------
        PermutiveBadRequestError
            For HTTP 400 errors.
        PermutiveAuthenticationError
            For HTTP 401 or 403 errors.
        PermutiveResourceNotFoundError
            For HTTP 404 errors.
        PermutiveRateLimitError
            For HTTP 429 errors (after retries).
        PermutiveServerError
            For HTTP 5xx errors (after retries).
        PermutiveAPIError
            For other unexpected errors.
        """
        return RequestHelper.get_static(self.api_key, url)

    def post(self, url: str, data: dict) -> Response:
        """
        Perform a POST request using the instance's API key.

        Parameters
        ----------
        url : str
            The URL to send the request to.
        data : dict
            The data to send in the request body.

        Returns
        -------
        requests.Response
            The response from the server.

        Raises
        ------
        PermutiveBadRequestError
            For HTTP 400 errors.
        PermutiveAuthenticationError
            For HTTP 401 or 403 errors.
        PermutiveResourceNotFoundError
            For HTTP 404 errors.
        PermutiveRateLimitError
            For HTTP 429 errors (after retries).
        PermutiveServerError
            For HTTP 5xx errors (after retries).
        PermutiveAPIError
            For other unexpected errors.
        """
        return RequestHelper.post_static(self.api_key, url, data)

    def patch(self, url: str, data: dict) -> Response:
        """
        Perform a PATCH request using the instance's API key.

        Parameters
        ----------
        url : str
            The URL to send the request to.
        data : dict
            The data to send in the request body.

        Returns
        -------
        requests.Response
            The response from the server.

        Raises
        ------
        PermutiveBadRequestError
            For HTTP 400 errors.
        PermutiveAuthenticationError
            For HTTP 401 or 403 errors.
        PermutiveResourceNotFoundError
            For HTTP 404 errors.
        PermutiveRateLimitError
            For HTTP 429 errors (after retries).
        PermutiveServerError
            For HTTP 5xx errors (after retries).
        PermutiveAPIError
            For other unexpected errors.
        """
        return RequestHelper.patch_static(self.api_key, url, data)

    def delete(self, url: str) -> Response:
        """
        Perform a DELETE request using the instance's API key.

        Parameters
        ----------
        url : str
            The URL to send the request to.

        Returns
        -------
        requests.Response
            The response from the server.

        Raises
        ------
        PermutiveBadRequestError
            For HTTP 400 errors.
        PermutiveAuthenticationError
            For HTTP 401 or 403 errors.
        PermutiveResourceNotFoundError
            For HTTP 404 errors.
        PermutiveRateLimitError
            For HTTP 429 errors (after retries).
        PermutiveServerError
            For HTTP 5xx errors (after retries).
        PermutiveAPIError
            For other unexpected errors.
        """
        return RequestHelper.delete_static(self.api_key, url)

    # -------- Payload Helper --------
    @staticmethod
    def to_payload_static(
        dataclass_obj: Any, api_payload: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Convert a dataclass object to a dictionary payload.

        Fields with ``None`` values are omitted. If ``api_payload`` is provided,
        only keys included in this list are kept.

        Parameters
        ----------
        dataclass_obj : Any
            The dataclass object to convert.
        api_payload : Optional[List[str]], optional
            A list of keys to include in the payload. Defaults to None.

        Returns
        -------
        Dict[str, Any]
            The dictionary payload.
        """
        dataclass_dict = vars(dataclass_obj)
        filtered_dict = {
            k: v
            for k, v in dataclass_dict.items()
            if v is not None and (api_payload is None or k in api_payload)
        }
        filtered_dict_string = json.dumps(
            filtered_dict, indent=4, cls=customJSONEncoder
        )
        return json.loads(filtered_dict_string)

    # -------- Retry Wrapper --------
    @staticmethod
    def _with_retry(method: Callable, url: str, api_key: str, **kwargs) -> Response:
        """Retry logic for transient errors and rate limiting.

        Raises
        ------
        PermutiveBadRequestError
            For HTTP 400 errors.
        PermutiveAuthenticationError
            For HTTP 401 or 403 errors.
        PermutiveResourceNotFoundError
            For HTTP 404 errors.
        PermutiveRateLimitError
            For HTTP 429 errors (after retries).
        PermutiveServerError
            For HTTP 5xx errors (after retries).
        PermutiveAPIError
            For other unexpected errors.
        """
        params = (kwargs.pop("params", {}) or {}).copy()
        params["k"] = api_key
        kwargs["params"] = params

        attempt = 0
        delay = RequestHelper.INITIAL_DELAY
        response: Optional[Response] = None

        while attempt < RequestHelper.MAX_RETRIES:
            try:
                response = method(url, headers=RequestHelper.DEFAULT_HEADERS, **kwargs)
                assert response is not None
                if response.status_code in RequestHelper.SUCCESS_RANGE:
                    return response

                # Handle transient errors & rate limiting
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
                    delay *= RequestHelper.BACKOFF_FACTOR
                else:
                    # Non-retryable error
                    RequestHelper.handle_exception(
                        RequestException(f"HTTP {response.status_code}"), response
                    )

            except RequestException as e:
                if attempt >= RequestHelper.MAX_RETRIES - 1:
                    # Raise our custom error from the original exception
                    raise PermutiveAPIError(
                        f"Request failed after {attempt+1} attempts: {e}"
                    ) from e
                logging.warning(
                    f"Request failed ({e}), retrying in {delay}s (attempt {attempt+1})"
                )
                time.sleep(delay)
                delay *= RequestHelper.BACKOFF_FACTOR

            attempt += 1

        # This part should be unreachable if logic is correct, but as a safeguard:
        RequestHelper.handle_exception(
            RequestException("Max retries reached"), response
        )
        raise PermutiveServerError(
            "Max retries reached, but handle_exception did not raise."
        )  # pragma: no cover

    # -------- Exception Handling --------
    @staticmethod
    def _redact_sensitive_data(message: str, response: Response) -> str:
        # Redact sensitive data from both the message and any query parameters in the URL
        if hasattr(response, "request") and hasattr(response.request, "url"):
            parsed_url = urllib.parse.urlparse(response.request.url)
            query_params = urllib.parse.parse_qs(str(parsed_url.query))
            for key in RequestHelper.SENSITIVE_QUERY_KEYS:
                if key in query_params:
                    for secret in query_params[key]:
                        if secret:
                            message = message.replace(secret, "[REDACTED]")
        # Also redact any sensitive keys that may appear in the message as key-value pairs
        for key in RequestHelper.SENSITIVE_QUERY_KEYS:
            # Redact patterns like 'key=secret' or '"key": "secret"'
            # key=secret (in URLs)
            message = re.sub(
                rf"({key})=([^\s&]+)",
                rf"\1=[REDACTED]",
                message,
                flags=re.IGNORECASE,
            )
            # "key": "secret" (in JSON)
            message = re.sub(
                rf'("{key}"\s*:\s*")[^"]+(")',
                rf"\1[REDACTED]\2",
                message,
                flags=re.IGNORECASE,
            )
        return message

    @staticmethod
    def _extract_error_message(response: Response) -> str:
        try:
            error_content = json.loads(response.content)
            if isinstance(error_content, dict):
                error_details = error_content.get("error")
                if isinstance(error_details, dict):
                    return error_details.get("cause", "Unknown error")
            return "Unknown error"
        except json.JSONDecodeError:
            return "Could not parse error message"

    @staticmethod
    def handle_exception(e: Exception, response: Optional[Response]) -> None:
        """Handle exceptions and HTTP errors by raising custom exceptions.

        Parameters
        ----------
        e : Exception
            The exception to handle.
        response : Optional[Response]
            The HTTP response.

        Raises
        ------
        PermutiveBadRequestError
            For HTTP 400 errors.
        PermutiveAuthenticationError
            For HTTP 401 or 403 errors.
        PermutiveResourceNotFoundError
            For HTTP 404 errors.
        PermutiveRateLimitError
            For HTTP 429 errors (after retries).
        PermutiveServerError
            For HTTP 5xx errors (after retries).
        PermutiveAPIError
            For other unexpected errors.
        """
        if response:
            status = response.status_code

            if status in RequestHelper.SUCCESS_RANGE:
                # This should not be reached if called from _with_retry
                return  # pragma: no cover

            if status == 400:
                msg = RequestHelper._extract_error_message(response)
                msg = RequestHelper._redact_sensitive_data(msg, response)
                redacted_url = ""
                if hasattr(response, "request") and response.request.url:
                    redacted_url = RequestHelper._redact_sensitive_data(
                        response.request.url, response
                    )
                full_msg = f"400 Bad Request: {msg}" + (
                    f" [URL: {redacted_url}]" if redacted_url else ""
                )
                raise PermutiveBadRequestError(full_msg) from e

            if status == 401 or status == 403:
                raise PermutiveAuthenticationError(
                    f"{status}: Invalid API key or insufficient permissions."
                ) from e

            if status == 404:
                raise PermutiveResourceNotFoundError("Resource not found.") from e

            if status == 429:
                raise PermutiveRateLimitError("Retry limit exceeded.") from e

            if 500 <= status < 600:
                raise PermutiveServerError(
                    f"{status}: API unavailable after retries."
                ) from e

        # Fallback for exceptions without a response object
        raise PermutiveAPIError(f"An unexpected error occurred: {e}") from e


def check_filepath(filepath: str) -> None:
    """Check if the directory of a filepath exists and create it if needed.

    Parameters
    ----------
    filepath : str
        The path to the file.
    """
    if (
        not os.path.exists(os.path.dirname(filepath))
        and len(os.path.dirname(filepath)) > 0
    ):
        os.makedirs(os.path.dirname(filepath), exist_ok=True)


def split_filepath(fullfilepath: str) -> Tuple[str, str, str]:
    """Split a filepath into its path, name, and extension.

    Parameters
    ----------
    fullfilepath : str
        The full path to the file.

    Returns
    -------
    tuple
        A tuple containing the path, name, and extension.
    """
    path = os.path.dirname(fullfilepath)
    name, ext = os.path.splitext(os.path.basename(fullfilepath))
    return path, name, ext


T_co = TypeVar("T_co")


def chunk_list(lst: List[T_co], n: int) -> List[List[T_co]]:
    """Split a list into chunks of a specified size.

    Parameters
    ----------
    lst : list
        The list to split.
    n : int
        The size of each chunk.

    Returns
    -------
    list
        A list of chunks.
    """
    return [lst[i : i + n] for i in range(0, len(lst), n)]


def convert_list(val: Union[str, List[Any]]) -> List[Any]:
    """Convert a string representation of a list into a list.

    Parameters
    ----------
    val : str
        The string to convert.

    Returns
    -------
    list
        The converted list.
    """
    if isinstance(val, str):
        return ast.literal_eval(val)
    else:
        return val


def compare_list(list1: List[str], list2: List[str]) -> bool:
    """Compare two lists for equality, ignoring order.

    Parameters
    ----------
    list1 : List[str]
        The first list.
    list2 : List[str]
        The second list.

    Returns
    -------
    bool
        ``True`` if the lists are equal, ``False`` otherwise.
    """
    return set(list1) == set(list2)


def merge_list(lst1: List, lst2: Optional[Union[int, str, List]] = None) -> List:
    """Merge two lists, removing duplicates and sorting the result.

    Parameters
    ----------
    lst1 : List
        The first list.
    lst2 : Optional[Union[int, str, List]], optional
        The second list. Defaults to None.

    Returns
    -------
    List
        The merged list.
    """
    if lst2 is None:
        lst2 = []
    elif isinstance(lst2, (str, int)):
        lst2 = [lst2]

    # Use a set for efficient merging and duplicate removal
    merged_set = set(lst1) | set(lst2)
    # Remove None if it exists
    merged_set.discard(None)

    return sorted(list(merged_set))


def load_json_list(
    data: Union[dict, List[dict], str, Path],
    list_name: str,
    item_name: Optional[str] = None,
) -> List[dict]:
    """Load a list of dictionaries from various JSON representations.

    Parameters
    ----------
    data : Union[dict, List[dict], str, Path]
        The JSON representation to load.
    list_name : str
        Name of the list class for error messages.
    item_name : Optional[str], optional
        Name of the item class for error messages. Defaults to the list name
        with ``'List'`` trimmed.

    Returns
    -------
    List[dict]
        The parsed list of dictionaries.

    Raises
    ------
    TypeError
        If ``data`` cannot be converted to a list of dictionaries.
    """
    if item_name is None and list_name.endswith("List"):
        item_name = list_name[:-4]

    if isinstance(data, dict):
        raise TypeError(
            (
                "Cannot create a {list_name} from a dictionary. "
                "Use from_json on the {item_name} class for single objects."
            ).format(list_name=list_name, item_name=item_name or "item")
        )

    if isinstance(data, (str, Path)):
        try:
            content = (
                data.read_text(encoding="utf-8") if isinstance(data, Path) else data
            )
            loaded = json.loads(content)
        except Exception as exc:  # pragma: no cover - error path
            raise TypeError(f"Failed to parse JSON from input: {exc}")
        if not isinstance(loaded, list):
            raise TypeError(
                ("JSON content from {kind} did not decode to a list.").format(
                    kind=type(data).__name__
                )
            )
        data = loaded

    if isinstance(data, list):
        return data

    raise TypeError(
        (
            "`from_json()` expected a list of dicts, JSON string, or Path, "
            "but got {kind}"
        ).format(kind=type(data).__name__)
    )


T = TypeVar("T", bound="JSONSerializable")
JSONOutput = TypeVar("JSONOutput", Dict[str, Any], List[Any])


def json_default(value: Any):
    """Provide JSON serialization for complex data types."""
    if isinstance(value, Enum):
        return value.value
    elif isinstance(value, (float, Decimal)):
        return float(value)
    elif isinstance(value, (int)):
        return int(value)
    elif isinstance(value, uuid.UUID):
        return str(value)  # Convert UUID to string
    elif isinstance(value, datetime.datetime):
        return value.isoformat()
    elif isinstance(value, datetime.date):
        return {"year": value.year, "month": value.month, "day": value.day}
    elif isinstance(value, (list, set, tuple)):
        # Recursively process items
        return [json_default(item) for item in value]
    elif isinstance(value, dict):
        # Handle dicts recursively
        return {k: json_default(v) for k, v in value.items()}
    elif hasattr(value, "__dict__"):
        # Convert objects to dict
        return {k: json_default(v) for k, v in value.__dict__.items()}
    elif value is None:
        return None
    else:
        return str(value)


class customJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for complex data types."""

    def default(self, o):
        """Override the default JSON encoder to handle complex data types."""
        try:
            return json_default(o)
        except TypeError:
            return super().default(o)


class JSONSerializable(Generic[JSONOutput]):
    """
    Mixin providing JSON serialization and deserialization capabilities.

    This is a generic mixin that should be used with a type argument specifying
    the output of `to_json`, e.g., `JSONSerializable[Dict[str, Any]]`.

    Methods
    -------
    __str__() -> str
        Pretty-print JSON when calling print(object).
    to_json() -> JSONOutput
        Convert the object to a JSON-serializable format.
    from_json(cls, data: dict) -> T
        Create an instance of the class from a JSON dictionary.
    to_json_file(filepath: str)
        Serialize the object to a JSON file using CustomJSONEncoder.
    from_json_file(cls, filepath: str) -> T
        Create an instance of the class from a JSON file.
    """

    def __str__(self) -> str:
        """Pretty-print JSON when calling print(object)."""
        return json.dumps(self.to_json(), indent=4, cls=customJSONEncoder)

    def to_json(self) -> JSONOutput:
        """Convert the object to a JSON-serializable format."""

        # Case 1: if self is a dict-like object
        if isinstance(self, dict):
            return cast(
                JSONOutput,
                {
                    k: JSONSerializable.serialize_value(v)
                    for k, v in self.items()
                    if not str(k).startswith("_")
                },
            )

        # Case 2: if self is a list-like object
        elif isinstance(self, list):
            return cast(
                JSONOutput,
                [
                    JSONSerializable.serialize_value(item)
                    for item in self
                    if item not in (None, [], {})
                ],
            )

        # Case 3: if self is a dataclass
        elif is_dataclass(self):
            result = {}
            for f in fields(self):
                try:
                    value = getattr(self, f.name)
                    serialized = JSONSerializable.serialize_value(value)
                    if serialized not in (None, [], {}):
                        result[f.name] = serialized
                except Exception as e:
                    logging.warning(f"Error serializing field {f.name}: {e}")
            return cast(JSONOutput, result)

        # Case 4: fallback to __dict__ if available
        elif hasattr(self, "__dict__"):
            return cast(
                JSONOutput,
                {
                    k: JSONSerializable.serialize_value(v)
                    for k, v in self.__dict__.items()
                    if not k.startswith("_")
                },
            )

        # Final fallback for unsupported objects — raise error instead of returning str/float
        raise TypeError(f"{type(self).__name__} is not JSON-serializable")

    @staticmethod
    def serialize_value(value: Any) -> Union[Dict[str, Any], List[Any], str, int, float, None]:
        """
        Convert a Python value into a JSON-compatible representation.

        This is the counterpart to ``unserialize_value`` used by ``from_json``.
        It handles nested JSONSerializable objects, lists, and dictionaries,
        while dropping empty values for cleanliness.

        Parameters
        ----------
        value : Any
            The Python value to convert.

        Returns
        -------
        Dict[str, Any] | List[Any] | str | int | float | None
            A JSON-serializable value suitable for dumping.
        """
        if isinstance(value, JSONSerializable):
            return value.to_json()
        if isinstance(value, list):
            return [
                JSONSerializable.serialize_value(item)
                for item in value
                if item not in (None, [], {})
            ]
        if isinstance(value, dict):
            return {
                key: JSONSerializable.serialize_value(val)
                for key, val in value.items()
                if val not in (None, [], {})
            }
        return json_default(value)

    def to_json_file(self, filepath: str):
        """
        Serialize the object to a JSON file.

        Parameters
        ----------
        filepath : str
            The path to the output JSON file.
        """
        check_filepath(filepath=filepath)
        with open(file=filepath, mode="w", encoding="utf-8") as f:
            json.dump(
                self.to_json(), f, ensure_ascii=False, indent=4, cls=customJSONEncoder
            )

    @classmethod
    def from_json_file(cls: Type[T], filepath: str) -> T:
        """
        Create an instance of the class from a JSON file.

        Parameters
        ----------
        filepath : str
            The path to the input JSON file.

        Returns
        -------
        T
            An instance of the class.
        """
        return cls.from_json(Path(filepath))

    @classmethod
    def from_json(cls: Type[T], data: Union[Dict[str, Any], str, Path]) -> T:
        """
        Create an instance from a dictionary, JSON string, or file path.

        Parameters
        ----------
        data : Union[Dict[str, Any], str, Path]
            The data to deserialize. Can be a dictionary, a JSON-formatted
            string, or a path to a JSON file.

        Returns
        -------
        T
            An instance of the class.

        Raises
        ------
        TypeError
            If the input data is not a dict, string, or Path, or if parsing fails.
        """
        # Load if input is a string or path
        if isinstance(data, (str, Path)):
            try:
                if isinstance(data, Path):
                    content = data.read_text(encoding="utf-8")
                else:
                    content = data
                data = json.loads(content)
            except Exception as e:
                raise TypeError(f"Failed to parse JSON from input: {e}")

        # Single dict
        if isinstance(data, dict):
            # If cls is a dataclass, coerce field values by annotation.
            if is_dataclass(cls):
                module = sys.modules.get(cls.__module__)
                kwargs: Dict[str, Any] = {}
                for f in fields(cls):
                    if f.name in data:
                        kwargs[f.name] = JSONSerializable.unserialize_value(
                            data[f.name], f.type, module
                        )
                return cls(**kwargs)

            # Fallback: construct directly
            return cls(**data)

        raise TypeError(
            f"`from_json()` expected a dict, JSON string, or Path, but got {type(data).__name__}"
        )

    @staticmethod
    def unserialize_value(value: Any, annotation: Any, module: Optional[Any] = None) -> Any:
        """
        Convert a JSON value into the annotated Python type.

        This helper mirrors the ``serialize_value`` pattern used in ``to_json``
        and centralizes all coercion logic (datetimes, nested dataclasses,
        lists of typed items, and forward references).

        Parameters
        ----------
        value : Any
            The raw JSON value to convert.
        annotation : Any
            The target type annotation (e.g., ``datetime``, ``Optional[datetime]``,
            ``List[Alias]``, or a ``JSONSerializable`` subclass).
        module : Optional[Any], optional
            Module used to resolve forward-referenced annotations written as
            strings. Defaults to ``None``.

        Returns
        -------
        Any
            The converted value when a conversion is applicable, otherwise the
            original value.
        """
        # Resolve forward references like "Source" -> actual class
        if isinstance(annotation, str) and module is not None:
            annotation = getattr(module, annotation, annotation)

        def is_datetime_type(tp: Any) -> bool:
            if tp is datetime.datetime:
                return True
            origin = get_origin(tp)
            if origin is Union:
                return any(arg is datetime.datetime for arg in get_args(tp))
            return False

        def parse_iso_datetime(val: Any) -> Any:
            if isinstance(val, str):
                try:
                    iso = val.replace("Z", "+00:00")
                    return datetime.datetime.fromisoformat(iso)
                except Exception:
                    return val
            return val

        def is_jsonserializable_subclass(tp: Any) -> bool:
            try:
                return isinstance(tp, type) and issubclass(tp, JSONSerializable)
            except Exception:
                return False

        def is_classinfo(obj: Any) -> bool:
            """Return True if obj can be used as the second arg to isinstance."""
            if isinstance(obj, type):
                return True
            if isinstance(obj, tuple):
                return all(isinstance(x, type) for x in obj)
            return False

        # If already of the right type (when possible), return
        if is_classinfo(annotation):
            classinfo = cast(Union[type, Tuple[type, ...]], annotation)
            if isinstance(value, classinfo):
                return value

        # Datetime coercion
        if is_datetime_type(annotation):
            return parse_iso_datetime(value)

        origin = get_origin(annotation)
        args = get_args(annotation)

        # List[T]
        if origin in (list, List) and isinstance(value, list) and args:
            return [JSONSerializable.unserialize_value(v, args[0], module) for v in value]

        # Dict[...] – leave as-is (no strong schema)
        if origin in (dict, Dict) and isinstance(value, dict):
            return value

        # Nested JSONSerializable dataclass
        if is_jsonserializable_subclass(annotation) and isinstance(value, dict):
            annot_cls = cast(Any, annotation)
            return annot_cls.from_json(value)

        return value
