import requests
from requests.exceptions import RequestException
from requests.models import Response
import json
import pathlib
from glob import glob
import ast
from typing import Dict, List, Optional, Any, Union, Type, TypeVar, get_args, overload
import logging
import os
from dataclasses import dataclass, is_dataclass, fields, Field as DataclassField
import uuid
from decimal import Decimal
from enum import Enum
import datetime
from pathlib import Path


class RequestHelper:
    """
        A utility class for making HTTP requests to a RESTful API and handling common operations.

        Attributes:
            DEFAULT_HEADERS (dict): Default HTTP headers used for API requests.
    """
    DEFAULT_HEADERS = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    api_key: str
    api_endpoint: str
    payload_keys: Optional[List[str]] = None

    def __init__(self,
                 api_key: str,
                 api_endpoint: str,
                 payload_keys: Optional[List[str]] = None) -> None:
        self.api_key = api_key
        self.api_endpoint = api_endpoint
        self.payload_keys = payload_keys

    @staticmethod
    def generate_url_with_key(url, api_key):
        if "?" in url:
            return f"{url}&k={api_key}"
        else:
            return f"{url}?k={api_key}"

    @staticmethod
    def get_static(api_key: str,
                   url: str) -> Response:
        response = None
        url = RequestHelper.generate_url_with_key(url, api_key)
        try:
            response = requests.get(
                url, headers=RequestHelper.DEFAULT_HEADERS)
            response.raise_for_status()
        except RequestException as e:
            return RequestHelper.handle_exception(e=e,
                                                  response=response)
        return response

    @staticmethod
    def post_static(api_key: str,
                    url: str,
                    data: dict) -> Response:
        """
            Send an HTTP POST request to the specified URL with JSON data.

            Args:
                url (str): The URL to send the POST request to.
                data (dict): The JSON data to include in the request body.
                headers (Optional[Dict[str, str]]): Custom headers to include in the request. Defaults to None.

            Returns:
                Response: The HTTP response object.

        """
        response = None
        url = RequestHelper.generate_url_with_key(url, api_key)
        try:
            response = requests.post(url,
                                     headers=RequestHelper.DEFAULT_HEADERS,
                                     json=data)
            response.raise_for_status()
        except RequestException as e:
            return RequestHelper.handle_exception(e=e,
                                                  response=response)
        return response

    @staticmethod
    def patch_static(api_key: str,
                     url: str,
                     data: dict) -> Response:
        """
            Send an HTTP PATCH request to the specified URL with JSON data.

            Args:
                url (str): The URL to send the PATCH request to.
                data (dict): The JSON data to include in the request body.
                headers (Optional[Dict[str, str]]): Custom headers to include in the request. Defaults to None.

            Returns:
                Response: The HTTP response object.

        """
        response = None
        url = RequestHelper.generate_url_with_key(url=url,
                                                  api_key=api_key)
        try:
            response = requests.patch(url,
                                      headers=RequestHelper.DEFAULT_HEADERS,
                                      json=data)
            response.raise_for_status()
        except RequestException as e:
            return RequestHelper.handle_exception(e=e,
                                                  response=response)
        return response

    @staticmethod
    def delete_static(api_key: str,
                      url: str) -> Response:
        """
            Send an HTTP DELETE request to the specified URL.

            Args:
                url (str): The URL to send the DELETE request to.
                headers (Optional[Dict[str, str]]): Custom headers to include in the request. Defaults to None.

            Returns:
                Response: The HTTP response object.

        """
        response = None
        url = RequestHelper.generate_url_with_key(
            url=url, api_key=api_key)
        try:
            response = requests.delete(url,
                                       headers=RequestHelper.DEFAULT_HEADERS)
            response.raise_for_status()
        except RequestException as e:
            return RequestHelper.handle_exception(e=e,
                                                  response=response)
        return response

    def get(self,
            url) -> Response:
        return RequestHelper.get_static(self.api_key, url)

    def post(self,
             url: str,
             data: dict) -> Response:
        """
            Send an HTTP POST request to the specified URL with JSON data.

            Args:
                url (str): The URL to send the POST request to.
                data (dict): The JSON data to include in the request body.
                headers (Optional[Dict[str, str]]): Custom headers to include in the request. Defaults to None.

            Returns:
                Response: The HTTP response object.

        """
        return RequestHelper.post_static(api_key=self.api_key,
                                         url=url,
                                         data=data)

    def patch(self,
              url: str,
              data: dict) -> Response:
        """
            Send an HTTP PATCH request to the specified URL with JSON data.

            Args:
                url (str): The URL to send the PATCH request Permutiveto.
                data (dict): The JSON data to include in the request body.
                headers (Optional[Dict[str, str]]): Custom headers to include in the request. Defaults to None.

            Returns:
                Response: The HTTP response object.

        """
        return RequestHelper.patch_static(api_key=self.api_key,
                                          url=url,
                                          data=data)

    def delete(self, url: str) -> Response:
        """
            Send an HTTP DELETE request to the specified URL.

            Args:
                url (str): The URL to send the DELETE request to.

            Returns:
                Response: The HTTP response object.

        """
        return RequestHelper.delete_static(api_key=self.api_key,
                                           url=url)

    @staticmethod
    def to_payload_static(dataclass_obj: Any, api_payload: Optional[List[str]] = None) -> Dict[str, Any]:
        """
            Convert a data class object to a dictionary payload.

            This method converts a data class object into a dictionary, optionally filtering keys.

            Args:
                dataclass_obj (Any): The data class object to be converted.
                keys (Optional[List[str]]): List of keys to include in the payload. If None, all keys with non-None values are included.

            Returns:
                Dict[str, Any]: The dictionary payload.

        """
        dataclass_dict = vars(dataclass_obj)
        filtered_dict = {key: value for key, value in dataclass_dict.items(
        ) if value and (api_payload is None or key in api_payload)}

        # Serialize using the custom serializer
        filtered_dict_string = json.dumps(filtered_dict,
                                          indent=4,
                                          cls=customJSONEncoder)
        return json.loads(filtered_dict_string)

    @staticmethod
    def handle_exception(e: Exception,
                         response: Optional[Response]
                         ):
        """
            Handle exceptions and errors in API requests.

            This method checks the HTTP response and handles exceptions gracefully. It logs error messages and raises exceptions when necessary.

            Args:
                e (Exception): The exception that occurred during the request.
                response (Optional[Response]): The HTTP response object.


            Returns:
                Response: The HTTP response object if it's successful or a 400 Bad Request response. Otherwise, it raises the original exception.

        """
        if response:
            if 200 <= response.status_code <= 300:
                return response
            elif response.status_code == 400:
                try:
                    error_content = json.loads(response.content)
                    error_message = error_content.get(
                        "error", {}).get("cause", "Unknown error")
                except json.JSONDecodeError:
                    error_message = "Could not parse error message"

                logging.warning(f"Received a 400 Bad Request: {error_message}")
                return response
        logging.error(f"An error occurred: {e}")
        raise e


class FileHelper:

    @staticmethod
    def check_filepath(filepath: str):
        if not os.path.exists(os.path.dirname(filepath)) and len(os.path.dirname(filepath)) > 0:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)

    @staticmethod
    def split_filepath(fullfilepath):
        p = pathlib.Path(fullfilepath)
        file_path = str(p.parent)+'/'
        file_name = p.name
        file_extension = ''
        for suffix in p.suffixes:
            file_name = file_name.replace(suffix, '')
            file_extension = file_extension+suffix
        return file_path, file_name, file_extension

    @staticmethod
    def file_exists(fullfilepath):
        file_path, file_name, file_extension = FileHelper.split_filepath(
            fullfilepath)

        if len(glob(f"{file_path}{file_name }-*{file_extension}") + glob(f"{file_path}{file_name}{file_extension}")) > 0:
            return True
        return False


class ListHelper:

    @staticmethod
    def chunk_list(lst, n):
        return [lst[i:i + n] for i in range(0, len(lst), n)]

    @staticmethod
    def convert_list(val):
        if isinstance(val, str):
            return ast.literal_eval(val)
        else:
            return val

    @staticmethod
    def compare_list(list1: List[str], list2: List[str]):
        return set(list1) == set(list2)

    @staticmethod
    def merge_list(lst1: List, lst2: Optional[Union[int, str, List]] = None) -> List:
        if isinstance(lst2, str) or isinstance(lst2, int):
            lst2 = [lst2]
        if not lst2:
            lst2 = []
        lst = list(filter(None, list(dict.fromkeys(lst1+lst2))))
        lst.sort()
        return lst


T = TypeVar('T', bound='JSONSerializable')


def json_default(value: Any):
    """Custom JSON serialization function for complex data types."""
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
    def default(self, o):
        try:
            return json_default(o)
        except TypeError:
            return super().default(o)


class JSONSerializable:
    """
    A mixin class that provides JSON serialization and deserialization capabilities.
    Methods
    -------
    __str__() -> str
        Pretty-print JSON when calling print(object).
    to_json() -> dict
        Converts the object to a JSON-serializable dictionary.
    from_json(cls, data: dict) -> T
        Creates an instance of the class from a JSON dictionary.
    to_json_file(filepath: str)
        Serializes the object to a JSON file using CustomJSONEncoder.
    from_json_file(cls, filepath: str) -> T
        Creates an instance of the class from a JSON file.
    """

    def __str__(self) -> str:
        """ Pretty-print JSON when calling print(object). """
        return json.dumps(self.to_json(),
                          indent=4,
                          cls=customJSONEncoder)

    def to_json(self) -> Union[dict[str, Any], list[Any]]:
        """Converts the object to a JSON-serializable format."""

        def serialize_value(v):
            if isinstance(v, JSONSerializable):
                return v.to_json()
            elif isinstance(v, list):
                return [serialize_value(item) for item in v if item not in (None, [], {})]
            elif isinstance(v, dict):
                return {key: serialize_value(value) for key, value in v.items() if value not in (None, [], {})}
            else:
                return json_default(v)

        # Case 1: if self is a dict-like object
        if isinstance(self, dict):
            return {k: serialize_value(v) for k, v in self.items() if not str(k).startswith("_")}

        # Case 2: if self is a list-like object
        elif isinstance(self, list):
            return [serialize_value(item) for item in self if item not in (None, [], {})]

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
            return {k: serialize_value(v) for k, v in self.__dict__.items() if not k.startswith("_")}

        # Final fallback for unsupported objects — raise error instead of returning str/float
        raise TypeError(f"{type(self).__name__} is not JSON-serializable")


    def to_json_file(self, filepath: str):
        """Serializes the object to a JSON file using CustomJSONEncoder."""
        FileHelper.check_filepath(filepath=filepath)
        with open(file=filepath, mode='w', encoding='utf-8') as f:
            json.dump(self.to_json(), f, ensure_ascii=False,
                      indent=4, cls=customJSONEncoder)

    @classmethod
    def from_json_file(cls: Type[T], filepath: str) -> T:
        """Creates an instance of the class from a JSON file."""
        with open(file=filepath, mode='r') as json_file:
            data = json.load(json_file)
            return cls.from_json(data)

    @overload
    @classmethod
    def from_json(cls: Type[T], data: dict) -> T: ...

    @overload
    @classmethod
    def from_json(cls: Type[T], data: list[dict]) -> list[T]: ...

    @overload
    @classmethod
    def from_json(cls: Type[T], data: str) -> Union[T, List[T]]: ...

    @overload
    @classmethod
    def from_json(cls: Type[T], data: Path) -> Union[T, List[T]]: ...

    @classmethod
    def from_json(cls: Type[T], data: Any) -> Union[T, List[T]]:
        """Handles JSON deserialization from dict, list[dict], JSON string, or file path."""

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

        # If list of dicts and cls is a list subclass
        if isinstance(data, list):
            if issubclass(cls, list):
                try:
                    # type: ignore[attr-defined]
                    base_args = get_args(cls.__orig_bases__[0]) # type: ignore
                    if not base_args:
                        raise TypeError(
                            "Cannot determine list item type for deserialization")
                    item_type = base_args[0]
                except Exception as e:
                    raise TypeError(
                        f"Failed to resolve list item type for {cls.__name__}: {e}")
                # type: ignore
                return cls([item_type.from_json(item) for item in data])
            else:
                return [cls.from_json(item) for item in data]

        # Single dict
        if isinstance(data, dict):
            return cls(**data)

        raise TypeError(
            f"`from_json()` expected a dict, list of dicts, JSON string, or Path, but got {type(data).__name__}"
        )
