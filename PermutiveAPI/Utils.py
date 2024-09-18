import requests
from requests.exceptions import RequestException
from requests.models import Response
import json
import datetime
import pathlib
from glob import glob
import ast
from typing import Dict, List, Optional, Any, Union
import logging
import os


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
    def gen_url_with_key(url, privateKey):
        if "?" in url:
            return f"{url}&k={privateKey}"
        else:
            return f"{url}?k={privateKey}"

    @staticmethod
    def getRequest_static(privateKey: str, url: str) -> Response:
        response = None
        url = RequestHelper.gen_url_with_key(url, privateKey)
        try:
            response = requests.get(
                url, headers=RequestHelper.DEFAULT_HEADERS)
            response.raise_for_status()
        except RequestException as e:
            return RequestHelper.handle_exception(response, e)
        return response

    @staticmethod
    def postRequest_static(privateKey: str,
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
        url = RequestHelper.gen_url_with_key(url, privateKey)
        try:
            response = requests.post(url,
                                     headers=RequestHelper.DEFAULT_HEADERS,
                                     json=data)
            response.raise_for_status()
        except RequestException as e:
            return RequestHelper.handle_exception(response, e)
        return response

    @staticmethod
    def patchRequest_static(privateKey: str,
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
        url = RequestHelper.gen_url_with_key(url=url,
                                             privateKey=privateKey)
        try:
            response = requests.patch(url,
                                      headers=RequestHelper.DEFAULT_HEADERS,
                                      json=data)
            response.raise_for_status()
        except RequestException as e:
            return RequestHelper.handle_exception(response, e)
        return response

    @staticmethod
    def deleteRequest_static(privateKey: str, url: str) -> Response:
        """
            Send an HTTP DELETE request to the specified URL.

            Args:
                url (str): The URL to send the DELETE request to.
                headers (Optional[Dict[str, str]]): Custom headers to include in the request. Defaults to None.

            Returns:
                Response: The HTTP response object.

        """
        response = None
        url = RequestHelper.gen_url_with_key(
            url=url, privateKey=privateKey)
        try:
            response = requests.delete(url,
                                       headers=RequestHelper.DEFAULT_HEADERS)
            response.raise_for_status()
        except RequestException as e:
            return RequestHelper.handle_exception(response, e)
        return response

    def getRequest(self,
                   url) -> Response:
        response = None
        url = RequestHelper.gen_url_with_key(
            url=url, privateKey=self.api_key)
        try:
            response = requests.get(url, headers=self.DEFAULT_HEADERS)
            response.raise_for_status()
        except RequestException as e:
            return RequestHelper.handle_exception(response, e)
        return response

    def postRequest(self,
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
        url = RequestHelper.gen_url_with_key(
            url=url, privateKey=self.api_key)
        try:
            response = requests.post(url,
                                     headers=self.DEFAULT_HEADERS,
                                     json=data)
            response.raise_for_status()
        except RequestException as e:
            return RequestHelper.handle_exception(response, e)
        return response

    def patchRequest(self,
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
        url = RequestHelper.gen_url_with_key(
            url=url, privateKey=self.api_key)
        try:
            response = requests.patch(url,
                                      headers=self.DEFAULT_HEADERS,
                                      json=data)
            response.raise_for_status()
        except RequestException as e:
            return RequestHelper.handle_exception(response, e)
        return response

    def deleteRequest(self, url: str) -> Response:
        """
            Send an HTTP DELETE request to the specified URL.

            Args:
                url (str): The URL to send the DELETE request to.
                headers (Optional[Dict[str, str]]): Custom headers to include in the request. Defaults to None.

            Returns:
                Response: The HTTP response object.

        """
        response = None
        url = RequestHelper.gen_url_with_key(
            url=url, privateKey=self.api_key)
        try:
            response = requests.delete(url, headers=self.DEFAULT_HEADERS)
            response.raise_for_status()
        except RequestException as e:
            return RequestHelper.handle_exception(response, e)
        return response

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
        final_dict = json.loads(json.dumps(
            filtered_dict, default=FileHelper.json_default))
        return final_dict

    @staticmethod
    def handle_exception(response: Optional[Response], e: Exception):
        """
            Handle exceptions and errors in API requests.

            This method checks the HTTP response and handles exceptions gracefully. It logs error messages and raises exceptions when necessary.

            Args:
                response (Optional[Response]): The HTTP response object.
                e (Exception): The exception that occurred during the request.

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
    def json_default(value):
        if isinstance(value, datetime.date):
            return dict(year=value.year, month=value.month, day=value.day)
        elif isinstance(value, list):
            return [FileHelper.json_default(item) for item in value]
        else:
            return value.__dict__

    def to_json(self, filepath: str):
        FileHelper.check_filepath(filepath)
        with open(file=filepath, mode='w', encoding='utf-8') as f:
            json.dump(self, f,
                      ensure_ascii=False, indent=4, default=FileHelper.json_default)

    @staticmethod
    def from_json(filepath: str):
        if not FileHelper.file_exists(filepath):
            raise ValueError(f'{filepath} does not exist')
        with open(file=filepath, mode='r') as json_file:
            return json.load(json_file)

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
