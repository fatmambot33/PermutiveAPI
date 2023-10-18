import logging
import json
import requests
from requests.exceptions import RequestException
from typing import Dict, Optional, Any
from aiohttp import ClientSession, TCPConnector, ClientTimeout
import asyncio


class APIRequestHandler:
    DEFAULT_HEADERS = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

    def __init__(self, max_parallel_requests=10):
        self.loop = asyncio.get_event_loop()
        self.connector = TCPConnector(limit=max_parallel_requests)
        self.timeout = ClientTimeout(total=30)
        self.session = ClientSession(
            connector=self.connector, timeout=self.timeout)
        self.loop.run_until_complete(self.session.__aenter__())

    # Synchronous GET method
    @staticmethod
    def get(url: str, headers: Optional[Dict[str, str]] = None) -> Any:
        headers = headers or APIRequestHandler.DEFAULT_HEADERS
        response = None
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except RequestException as e:
            return APIRequestHandler.handle_exception(response, e)

    # Asynchronous GET method
    async def async_get(self, url: str, headers: Optional[Dict[str, str]] = None) -> Any:
        headers = headers or APIRequestHandler.DEFAULT_HEADERS
        response = None
        try:
            async with self.session.get(url, headers=headers) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            return await APIRequestHandler.handle_exception(response, e)

    @staticmethod
    def handle_exception(response, e: Exception) -> Any:
        if response is not None:
            if 200 <= response.status_code <= 300:
                return response.json()
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


# Example of usage
if __name__ == "__main__":
    handler = APIRequestHandler()

    # Synchronous request
    sync_response = handler.get("http://example.com/api/resource1")
    print(f"Synchronous response: {sync_response}")

    # Asynchronous request
    async_response = handler.loop.run_until_complete(
        handler.async_get("http://example.com/api/resource2"))
    print(f"Asynchronous response: {async_response}")
