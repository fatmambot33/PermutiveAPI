"""
APIRequestHandler Documentation

This module provides a utility class for making HTTP requests to a RESTful API. It offers a set of methods for making GET, POST, PATCH, and DELETE requests, handling exceptions, and converting data objects into payload dictionaries.

Usage:
    To use this class, you can create an instance of the `APIRequestHandler` class, or you can directly call its static methods.

Example:
    # Instantiate the class
    api_handler = APIRequestHandler()

    # Make a GET request
    response = api_handler.get("https://api.example.com/data")

    # Make a POST request
    data = {"key": "value"}
    response = api_handler.post("https://api.example.com/create", data)

Static Methods:
    - `get(url: str, headers: Optional[Dict[str, str]] = None) -> Response`:
        Makes a GET request to the specified URL.
    
    - `post(url: str, data: dict, headers: Optional[Dict[str, str]] = None) -> Response`:
        Makes a POST request to the specified URL with the provided data.

    - `patch(url: str, data: dict, headers: Optional[Dict[str, str]] = None) -> Response`:
        Makes a PATCH request to the specified URL with the provided data.

    - `delete(url: str, headers: Optional[Dict[str, str]] = None) -> Response`:
        Makes a DELETE request to the specified URL.

    - `to_payload(dataclass_obj: Any, keys: Optional[List[str]] = None) -> Dict[str, Any`:
        Converts a data object into a dictionary (payload) for use in POST or PATCH requests. The `keys` parameter allows you to specify a list of keys to include in the payload.

    - `handle_exception(response: Optional[Response], e: Exception)`:
        A private method for handling exceptions. It logs errors and raises exceptions based on the HTTP response.

Class Attributes:
    - `DEFAULT_HEADERS`:
        Default HTTP headers for requests, set to accept JSON content and use JSON as content type.

Dependencies:
    This module relies on the `requests` library for making HTTP requests.


"""
import logging
from typing import Dict, List, Optional, Any

import requests
from requests.exceptions import RequestException
from requests.models import Response
import json


class APIRequestHandler:
    """
        A utility class for making HTTP requests to a RESTful API and handling common operations.

        Attributes:
            DEFAULT_HEADERS (dict): Default HTTP headers used for API requests.
    """
    DEFAULT_HEADERS = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

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
        if response is not None:
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

    @staticmethod
    def get(url: str, headers: Optional[Dict[str, str]] = None) -> Response:
        """
            Send an HTTP GET request to the specified URL.

            Args:
                url (str): The URL to send the GET request to.
                headers (Optional[Dict[str, str]]): Custom headers to include in the request. Defaults to None.

            Returns:
                Response: The HTTP response object.

        """
        headers = headers or APIRequestHandler.DEFAULT_HEADERS
        response = None
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
        except RequestException as e:
            return APIRequestHandler.handle_exception(response, e)
        return response

    @staticmethod
    def post(url: str, data: dict, headers: Optional[Dict[str, str]] = None) -> Response:
        """
            Send an HTTP POST request to the specified URL with JSON data.

            Args:
                url (str): The URL to send the POST request to.
                data (dict): The JSON data to include in the request body.
                headers (Optional[Dict[str, str]]): Custom headers to include in the request. Defaults to None.

            Returns:
                Response: The HTTP response object.

        """
        headers = headers or APIRequestHandler.DEFAULT_HEADERS
        response = None
        try:
            response = requests.post(url, headers=headers, json=data)
            response.raise_for_status()
        except RequestException as e:
            return APIRequestHandler.handle_exception(response, e)
        return response

    @staticmethod
    def patch(url: str, data: dict, headers: Optional[Dict[str, str]] = None) -> Response:
        """
            Send an HTTP PATCH request to the specified URL with JSON data.

            Args:
                url (str): The URL to send the PATCH request to.
                data (dict): The JSON data to include in the request body.
                headers (Optional[Dict[str, str]]): Custom headers to include in the request. Defaults to None.

            Returns:
                Response: The HTTP response object.

        """        
        headers = headers or APIRequestHandler.DEFAULT_HEADERS
        response = None
        try:
            response = requests.patch(url, headers=headers, json=data)
            response.raise_for_status()
        except RequestException as e:
            return APIRequestHandler.handle_exception(response, e)
        return response

    @staticmethod
    def delete(url: str, headers: Optional[Dict[str, str]] = None) -> Response:
        """
            Send an HTTP DELETE request to the specified URL.

            Args:
                url (str): The URL to send the DELETE request to.
                headers (Optional[Dict[str, str]]): Custom headers to include in the request. Defaults to None.

            Returns:
                Response: The HTTP response object.

        """        
        headers = headers or APIRequestHandler.DEFAULT_HEADERS
        response = None
        try:
            response = requests.delete(url, headers=headers)
            response.raise_for_status()
        except RequestException as e:
            return APIRequestHandler.handle_exception(response, e)
        return response

    @staticmethod
    def to_payload(dataclass_obj: Any, keys: Optional[List[str]] = None) -> Dict[str, Any]:
        """
            Convert a data class object to a dictionary payload.

            This method converts a data class object into a dictionary, optionally filtering keys.

            Args:
                dataclass_obj (Any): The data class object to be converted.
                keys (Optional[List[str]]): List of keys to include in the payload. If None, all keys with non-None values are included.

            Returns:
                Dict[str, Any]: The dictionary payload.

        """
        if keys:
            return {key: value for key, value in vars(dataclass_obj).items() if value is not None and key in keys}
        return {key: value for key, value in vars(dataclass_obj).items() if value is not None}
