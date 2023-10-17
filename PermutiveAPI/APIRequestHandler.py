import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Any

import requests
from requests.exceptions import RequestException
from requests.models import Response


class APIRequestHandler:
    DEFAULT_HEADERS = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

    @staticmethod
    def handle_exception(response: Optional[Response], e: Exception):
        if response is not None:
            logging.warning(response.content)
            if 200 <= response.status_code <= 300:
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
