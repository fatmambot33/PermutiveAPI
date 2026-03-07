"""Context segmentation helpers for the Permutive API.

This module wraps the Context API endpoint, allowing callers to submit a page
URL and associated page properties to retrieve contextual segment matches.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from PermutiveAPI._Utils import http
from PermutiveAPI._Utils.json import JSONSerializable

_API_ENDPOINT = "https://api.permutive.com/ctx/v1/segment"
_API_PAYLOAD = ["url", "page_properties"]


@dataclass
class ContextSegment(JSONSerializable[Dict[str, Any]]):
    """Submit contextual page data to the Permutive Context API.

    Parameters
    ----------
    url : str
        Canonical URL of the page being evaluated.
    page_properties : dict[str, Any]
        Additional page metadata, such as client details and content taxonomy.

    Methods
    -------
    to_json()
        Convert the request into the JSON payload accepted by the API.
    send(api_key, timeout=10.0)
        Dispatch the request to the context segmentation endpoint.

    Examples
    --------
    >>> request = ContextSegment(
    ...     url="https://example.com/article/sports-news",
    ...     page_properties={
    ...         "client": {"domain": "example.com", "type": "web"},
    ...         "category": "sports",
    ...     },
    ... )
    >>> request.send(api_key="permutive-api-key")  # doctest: +SKIP
    {"segments": []}
    """

    _request_helper = http

    url: str
    page_properties: Dict[str, Any]

    def to_json(self) -> Dict[str, Any]:
        """Return the JSON payload accepted by the context API."""
        return http.to_payload(self, _API_PAYLOAD)

    def send(
        self,
        api_key: str,
        *,
        timeout: Optional[float] = 10.0,
    ) -> Dict[str, Any]:
        """Submit the request to the context segmentation endpoint.

        Parameters
        ----------
        api_key : str
            API key used to authenticate with Permutive.
        timeout : float | None, optional
            Timeout in seconds for the HTTP request (default: ``10.0``).

        Returns
        -------
        dict[str, Any]
            Parsed JSON payload returned by the Permutive API.

        Raises
        ------
        PermutiveAPIError
            Propagated from the underlying HTTP helper when the API reports an
            error.
        """
        response = self._request_helper.request(
            method="POST",
            api_key=api_key,
            url=_API_ENDPOINT,
            json=self.to_json(),
            timeout=timeout,
        )
        return response.json()
