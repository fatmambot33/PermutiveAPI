

import logging

from typing import Dict, List, Optional, Any,Union
from dataclasses import asdict

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
    api_key:str
    api_endpoint:str
    payload_keys:Optional[List[str]]=None

    def __init__(self,
                 api_key:str,
                 api_endpoint:str,
                payload_keys:Optional[List[str]]=None) -> None:
        self.api_key=api_key
        self.api_endpoint=api_endpoint
        self.payload_keys=payload_keys
    def gen_url(self,url):
        if "?" in url:
            return f"{url}&"
        else:
            return f"{url}?"

    def getRequest(self,
                   url)-> Response:
        response = None
        url = f"{self.gen_url(url)}k={self.api_key}"
        try:
            response = requests.get(url, headers=self.DEFAULT_HEADERS)
            response.raise_for_status()
        except RequestException as e:
            return APIRequestHandler.handle_exception(response, e)
        return response

    
    def postRequest(self,
            url:str,
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
        url = f"{self.gen_url(url)}k={self.api_key}"
        try:
            response = requests.post(url,
                                    headers=self.DEFAULT_HEADERS,
                                    json=data)
            response.raise_for_status()
        except RequestException as e:
            return APIRequestHandler.handle_exception(response, e)
        return response


    def patchRequest(self,
                     url:str,
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
        url = f"{self.gen_url(url)}k={self.api_key}"
        try:
            response = requests.patch(url, 
                                      headers=self.DEFAULT_HEADERS, 
                                      json=data)
            response.raise_for_status()
        except RequestException as e:
            return APIRequestHandler.handle_exception(response, e)
        return response


    def deleteRequest(self,url:str) -> Response:
        """
            Send an HTTP DELETE request to the specified URL.

            Args:
                url (str): The URL to send the DELETE request to.
                headers (Optional[Dict[str, str]]): Custom headers to include in the request. Defaults to None.

            Returns:
                Response: The HTTP response object.

        """        
        response = None
        url = f"{self.gen_url(url)}k={self.api_key}"
        try:
            response = requests.delete(url, headers=self.DEFAULT_HEADERS)
            response.raise_for_status()
        except RequestException as e:
            return APIRequestHandler.handle_exception(response, e)
        return response

    def to_payload(self, dataclass_obj: Any) -> Dict[str, Any]:
            """
            Convert a data class object to a dictionary payload.
            
            Args:
                dataclass_obj (Any): The data class object to be converted.

            Returns:
                Dict[str, Any]: The dictionary payload.
            """
            full_payload = asdict(dataclass_obj)

            if self.payload_keys:
                payload = {key: value for key, value in full_payload.items() if value is not None and key in self.payload_keys}
                return payload

            return {key: value for key, value in full_payload.items() if value is not None}
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
