import logging
from typing import Dict, List, Optional, Any

import requests
from requests.exceptions import RequestException
from requests.models import Response
import json


class APIRequestHandler:
    DEFAULT_HEADERS = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

    @staticmethod
    def handle_exception(response: Optional[Response], e: Exception):
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
        if keys:
            return {key: value for key, value in vars(dataclass_obj).items() if value is not None and key in keys}
        return {key: value for key, value in vars(dataclass_obj).items() if value is not None}
