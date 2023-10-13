
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

import requests
from requests.exceptions import RequestException
from requests.models import Response


class APIRequestHandler:
    """
    APIRequestHandler class
    This class is responsible for sending GET, POST, PUT, DELETE requests and handling exceptions.
    """
    headers = {
        # 'Authorization': f'Bearer {api_key}',
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

    @staticmethod
    def get(url: str) -> Response:
        """
        Sends a GET request to the given URL.

        :param url: The URL to send the GET request to.
        :param headers: The headers of the request.
        :return: The response of the GET request.
        """
        logging.info(f"APIRequestHandler::get")
        try:
            response = requests.get(url, headers=APIRequestHandler.headers)
            response.raise_for_status()
        except RequestException as e:
            # Handle your exceptions as you see fit here
            if response is not None:
                if response.status_code <= 300:
                    if response.status_code >= 200:
                        logging.warning(response.content)
                    return response
            return None
        return response

    @staticmethod
    def post(url: str,  data: dict = None) -> Response:
        """
        Performs a POST request.

        :param url: The URL for the POST request.
        :param headers: The headers to send with the request.
        :param data: The data to send with the request.
        :return: The server's response to the request.
        """
        logging.info(f"APIRequestHandler::post")
        try:
            if data is None:
                raise ValueError('data must be specified')
            response: Response = requests.post(
                url, headers=APIRequestHandler.headers, json=data)
            response.raise_for_status()
        except RequestException as e:
            # Handle your exceptions as you see fit here
            if response.status_code <= 300:
                if response.status_code >= 200:
                    logging.warning(response.content)
                return response
            logging.warning(response.content)
            return None
        return response

    @staticmethod
    def patch(url: str, data: dict = None) -> Optional[Response]:
        """
        Performs a PUT request.

        :param url: The URL for the PUT request.
        :param headers: The headers to send with the request.
        :param data: The data to send with the request.
        :return: The server's response to the request.
        """
        logging.info(f"APIRequestHandler::patch")
        if data is None:
            raise ValueError('data must be specified')
        try:
            response: Response = requests.patch(
                url, headers=APIRequestHandler.headers, json=data)
            response.raise_for_status()
        except RequestException as e:
            if response is not None:
                logging.warning(response.content)
                if response.status_code <= 300:
                    return response
            return None
        return response

    @staticmethod
    def delete(url: str) -> Response:
        """
        Performs a DELETE request.

        :param url: The URL for the DELETE request.
        :param headers: The headers to send with the request.
        :return: The server's response to the request.
        """
        logging.info(f"APIRequestHandler::delete")
        try:
            response: Response = requests.delete(
                url, headers=APIRequestHandler.headers)
            response.raise_for_status()
        except RequestException as e:
            # Handle your exceptions as you see fit here
            if response.status_code <= 300:
                if response.status_code >= 200:
                    logging.warning(response.content)
                return response
            return None
        return response

    @staticmethod
    def to_payload(dataclass_obj: dataclass, keys: Optional[List[str]] = None) -> Dict[str, any]:
        if keys is not None:
            return {key: value for key, value in vars(dataclass_obj).items() if value is not None and key in keys}
        return {key: value for key, value in vars(dataclass_obj).items() if value is not None}
