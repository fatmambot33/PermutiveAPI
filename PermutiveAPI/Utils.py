"""Utility helpers for the Permutive API."""

import re
import requests
from requests.exceptions import RequestException
from requests.models import Response
import json
import pathlib
from glob import glob
import ast
from typing import Dict, List, Optional, Any, Union, Type, TypeVar, Callable, overload
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


class RequestHelper:
    """A utility class for making HTTP requests to a RESTful API."""

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

    # -------- URL Helper --------
    @staticmethod
    def generate_url_with_key(url: str, api_key: str) -> str:
        """
        Generate a URL with the API key appended as a ``k`` query parameter.

        Merges the key with any existing query parameters.

        Parameters
        ----------
        url : str
            The URL to append the key to.
        api_key : str
            The API key.

        Returns
        -------
        str
            The URL with the API key.
        """
        parsed_url = urllib.parse.urlparse(url)
        query = urllib.parse.parse_qs(parsed_url.query)
        query.update({"k": [api_key]})
        new_query = urllib.parse.urlencode(query, doseq=True)
        return urllib.parse.urlunparse(parsed_url._replace(query=new_query))

    # -------- Public Request Methods --------
    @staticmethod
    def get_static(api_key: str, url: str) -> Optional[Response]:
        """
        Perform a GET request with retry logic.

        Parameters
        ----------
        api_key : str
            The API key for authentication.
        url : str
            The URL to send the request to.

        Returns
        -------
        Optional[Response]
            The response from the server, or None if the request fails.
        """
        return RequestHelper._with_retry(requests.get, url, api_key)

    @staticmethod
    def post_static(api_key: str, url: str, data: dict) -> Optional[Response]:
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
        Optional[Response]
            The response from the server, or None if the request fails.
        """
        return RequestHelper._with_retry(requests.post, url, api_key, json=data)

    @staticmethod
    def patch_static(api_key: str, url: str, data: dict) -> Optional[Response]:
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
        Optional[Response]
            The response from the server, or None if the request fails.
        """
        return RequestHelper._with_retry(requests.patch, url, api_key, json=data)

    @staticmethod
    def delete_static(api_key: str, url: str) -> Optional[Response]:
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
        Optional[Response]
            The response from the server, or None if the request fails.
        """
        return RequestHelper._with_retry(requests.delete, url, api_key)

    def get(self, url) -> Optional[Response]:
        """
        Perform a GET request using the instance's API key.

        Parameters
        ----------
        url : str
            The URL to send the request to.

        Returns
        -------
        Optional[Response]
            The response from the server, or None if the request fails.
        """
        return RequestHelper.get_static(self.api_key, url)

    def post(self, url: str, data: dict) -> Optional[Response]:
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
        Optional[Response]
            The response from the server, or None if the request fails.
        """
        return RequestHelper.post_static(self.api_key, url, data)

    def patch(self, url: str, data: dict) -> Optional[Response]:
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
        Optional[Response]
            The response from the server, or None if the request fails.
        """
        return RequestHelper.patch_static(self.api_key, url, data)

    def delete(self, url: str) -> Optional[Response]:
        """
        Perform a DELETE request using the instance's API key.

        Parameters
        ----------
        url : str
            The URL to send the request to.

        Returns
        -------
        Optional[Response]
            The response from the server, or None if the request fails.
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
    def _with_retry(
        method: Callable, url: str, api_key: str, **kwargs
    ) -> Optional[Response]:
        """Retry logic for transient errors and rate limiting."""
        url = RequestHelper.generate_url_with_key(url, api_key)
        attempt = 0
        delay = RequestHelper.INITIAL_DELAY
        response = None

        while attempt < RequestHelper.MAX_RETRIES:
            try:
                response = method(url, headers=RequestHelper.DEFAULT_HEADERS, **kwargs)
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
                    return RequestHelper.handle_exception(
                        RequestException(f"HTTP {response.status_code}"), response
                    )

            except RequestException as e:
                if attempt >= RequestHelper.MAX_RETRIES - 1:
                    raise
                logging.warning(
                    f"Request failed ({e}), retrying in {delay}s (attempt {attempt+1})"
                )
                time.sleep(delay)
                delay *= RequestHelper.BACKOFF_FACTOR

            attempt += 1

        return RequestHelper.handle_exception(
            RequestException("Max retries reached"), response
        )

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
                rf'({key})=([^\s&"\']+)',
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
            return error_content.get("error", {}).get("cause", "Unknown error")
        except json.JSONDecodeError:
            return "Could not parse error message"

    @staticmethod
    def handle_exception(
        e: Exception, response: Optional[Response]
    ) -> Optional[Response]:
        """
        Handle exceptions and HTTP errors.

        Parameters
        ----------
        e : Exception
            The exception to handle.
        response : Optional[Response]
            The HTTP response.

        Returns
        -------
        Optional[Response]
            The response if it can be handled, otherwise None.
        """
        if response:
            status = response.status_code

            if status in RequestHelper.SUCCESS_RANGE:
                return response

            if status == 400:
                msg = RequestHelper._extract_error_message(response)
                # Redact sensitive data from both the error message and the request URL
                msg = RequestHelper._redact_sensitive_data(msg, response)
                redacted_url = ""
                if hasattr(response, "request") and response.request.url:
                    redacted_url = RequestHelper._redact_sensitive_data(
                        response.request.url, response
                    )
                logging.warning(
                    f"400 Bad Request: {msg}"
                    + (f" [URL: {redacted_url}]" if redacted_url else "")
                )
                return response

            if status == 401:
                logging.error("401 Unauthorized: Invalid API key or credentials.")
                return None

            if status == 403:
                logging.error(
                    "403 Forbidden: You don't have permission for this resource."
                )
                return None

            if status == 404:
                logging.warning("404 Not Found: Resource not found.")
                return None

            if status == 429:
                logging.error("429 Too Many Requests: Retry limit exceeded.")
                return None

            if 500 <= status < 600:
                logging.error(f"{status} Server Error: API unavailable after retries.")
                return None

        logging.error(f"An unexpected error occurred: {e}")
        raise e


def check_filepath(filepath: str):
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


def split_filepath(fullfilepath):
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
    p = pathlib.Path(fullfilepath)
    file_path = str(p.parent) + "/"
    file_name = p.name
    file_extension = ""
    for suffix in p.suffixes:
        file_name = file_name.replace(suffix, "")
        file_extension = file_extension + suffix
    return file_path, file_name, file_extension


def file_exists(fullfilepath: str) -> bool:
    """Check if a file exists, accounting for variations in the filename.

    Parameters
    ----------
    fullfilepath : str
        The full path to the file.

    Returns
    -------
    bool
        ``True`` if the file exists, ``False`` otherwise.
    """
    file_path, file_name, file_extension = split_filepath(fullfilepath)

    pattern_with_suffix = os.path.join(file_path, f"{file_name}-*{file_extension}")
    pattern_exact = os.path.join(file_path, f"{file_name}{file_extension}")
    return len(glob(pattern_with_suffix) + glob(pattern_exact)) > 0


def chunk_list(lst, n):
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


def convert_list(val):
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


def compare_list(list1: List[str], list2: List[str]):
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
    if isinstance(lst2, str) or isinstance(lst2, int):
        lst2 = [lst2]
    if not lst2:
        lst2 = []
    lst = list(filter(None, list(dict.fromkeys(lst1 + lst2))))
    lst.sort()
    return lst


T = TypeVar("T", bound="JSONSerializable")


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


class JSONSerializable:
    """
    Mixin providing JSON serialization and deserialization capabilities.

    Methods
    -------
    __str__() -> str
        Pretty-print JSON when calling print(object).
    to_json() -> dict
        Convert the object to a JSON-serializable dictionary.
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

    def to_json(self) -> Union[Dict[str, Any], List[Any]]:
        """Convert the object to a JSON-serializable format."""

        def serialize_value(v):
            if isinstance(v, JSONSerializable):
                return v.to_json()
            elif isinstance(v, list):
                return [
                    serialize_value(item) for item in v if item not in (None, [], {})
                ]
            elif isinstance(v, dict):
                return {
                    key: serialize_value(value)
                    for key, value in v.items()
                    if value not in (None, [], {})
                }
            else:
                return json_default(v)

        # Case 1: if self is a dict-like object
        if isinstance(self, dict):
            return {
                k: serialize_value(v)
                for k, v in self.items()
                if not str(k).startswith("_")
            }

        # Case 2: if self is a list-like object
        elif isinstance(self, list):
            return [
                serialize_value(item) for item in self if item not in (None, [], {})
            ]

        # Case 3: if self is a dataclass
        elif is_dataclass(self):
            result = {}
            for f in fields(self):
                try:
                    value = getattr(self, f.name)
                    serialized = serialize_value(value)
                    if serialized not in (None, [], {}):
                        result[f.name] = serialized
                except Exception as e:
                    logging.warning(f"Error serializing field {f.name}: {e}")
            return result

        # Case 4: fallback to __dict__ if available
        elif hasattr(self, "__dict__"):
            return {
                k: serialize_value(v)
                for k, v in self.__dict__.items()
                if not k.startswith("_")
            }

        # Final fallback for unsupported objects — raise error instead of returning str/float
        raise TypeError(f"{type(self).__name__} is not JSON-serializable")

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

    @overload
    @classmethod
    def from_json(cls: Type[T], data: dict) -> T: ...

    @overload
    @classmethod
    def from_json(cls: Type[T], data: str) -> T: ...

    @overload
    @classmethod
    def from_json(cls: Type[T], data: Path) -> T: ...

    @classmethod
    def from_json(cls: Type[T], data: Any) -> T:
        """
        Create an instance from a dictionary, JSON string, or file path.

        Parameters
        ----------
        data : Union[dict, str, Path]
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
            return cls(**data)

        raise TypeError(
            f"`from_json()` expected a dict, JSON string, or Path, but got {type(data).__name__}"
        )
