"""Segmentation helpers for the Permutive API.

This module wraps the CCS segmentation endpoint, allowing you to submit
arbitrary events for evaluation and retrieve the computed segment
membership in a structured way.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from requests import Response

from PermutiveAPI.Identify.Alias import Alias
from PermutiveAPI._Utils import http
from PermutiveAPI._Utils.http import BatchRequest, Progress, process_batch
from PermutiveAPI._Utils.json import JSONSerializable

_API_VERSION = "v1"
_API_ENDPOINT = f"https://api.permutive.app/ccs/{_API_VERSION}/segmentation"
_API_PAYLOAD = ["events", "user_id", "alias", "aliases"]


@dataclass
class Event(JSONSerializable[Dict[str, Any]]):
    """Represent a single event passed to the segmentation API.

    Parameters
    ----------
    name : str
        Event name to evaluate.
    time : str
        ISO-8601 timestamp describing when the event occurred.
    properties : dict[str, Any], optional
        Arbitrary event properties forwarded to Permutive (default: ``{}``).
    session_id : str | None, optional
        Session identifier associated with the event (default: ``None``).
    view_id : str | None, optional
        View identifier correlating events within the same page view (default: ``None``).

    Methods
    -------
    to_json()
        Convert the event into a JSON-serialisable dictionary.
    from_json(data)
        Create an event from a JSON dictionary.
    """

    name: str
    time: str
    properties: Dict[str, Any] = field(default_factory=dict)
    session_id: Optional[str] = None
    view_id: Optional[str] = None


@dataclass
class Segmentation(JSONSerializable[Dict[str, Any]]):
    """Submit events to the Permutive segmentation API.

    Parameters
    ----------
    events : list[Event]
        Collection of events that describe the interaction history.
    user_id : str | None, optional
        The Permutive user identifier to evaluate (default: ``None``).
    alias : Alias | None, optional
        A single alias used to identify the user (default: ``None``).
    aliases : list[Alias] | None, optional
        A prioritised list of aliases used to identify the user (default: ``None``).
    activations : bool, optional
        Whether to request activation results in the response (default: ``False``).
    synchronous_validation : bool, optional
        Whether to force synchronous payload validation (default: ``False``).

    Methods
    -------
    to_json()
        Convert the request into the JSON payload expected by the API.
    send(api_key, timeout=10.0)
        Dispatch the request to the segmentation endpoint and return the API response.
    batch_send(requests, api_key, max_workers=None, progress_callback=None, timeout=10.0)
        Dispatch multiple segmentation requests concurrently using the shared batch runner.
    """

    _request_helper = http

    events: List[Event]
    user_id: Optional[str] = None
    alias: Optional[Alias] = None
    aliases: Optional[List[Alias]] = None
    activations: bool = False
    synchronous_validation: bool = False

    def __post_init__(self) -> None:
        """Validate that exactly one user-identification field is provided.

        Raises
        ------
        ValueError
            Raised when none or more than one identifier fields are set.
        """
        provided_identifiers = [
            self.user_id is not None,
            self.alias is not None,
            self.aliases is not None,
        ]

        if sum(provided_identifiers) != 1:
            raise ValueError(
                "Provide exactly one of user_id, alias, or aliases for segmentation."
            )

    def to_json(self) -> Dict[str, Any]:
        """Return the JSON payload accepted by the segmentation API."""
        return http.to_payload(self, _API_PAYLOAD)

    def send(
        self,
        api_key: str,
        *,
        timeout: Optional[float] = 10.0,
    ) -> Dict[str, Any]:
        """Submit the request to the segmentation endpoint.

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
            Propagated from the underlying HTTP helper when the API reports an error.
        """
        params = {
            "activations": str(self.activations).lower(),
            "synchronous-validation": str(self.synchronous_validation).lower(),
        }
        response = self._request_helper.request(
            method="POST",
            api_key=api_key,
            url=_API_ENDPOINT,
            params=params,
            json=self.to_json(),
            timeout=timeout,
        )
        return response.json()

    @classmethod
    def batch_send(
        cls,
        requests: Iterable["Segmentation"],
        *,
        api_key: str,
        max_workers: Optional[int] = None,
        progress_callback: Optional[Callable[[Progress], None]] = None,
        timeout: Optional[float] = 10.0,
    ) -> Tuple[List[Dict[str, Any]], List[Tuple[BatchRequest, Exception]]]:
        """Submit multiple segmentation requests concurrently.

        Parameters
        ----------
        requests : Iterable[Segmentation]
            Segmentation payloads to dispatch.
        api_key : str
            API key used to authenticate with Permutive.
        max_workers : int | None, optional
            Maximum number of worker threads (default: ``None`` to defer to the
            shared batch runner's default).
        progress_callback : Callable[[Progress], None] | None, optional
            Invoked after each request completes. Receives a
            :class:`~PermutiveAPI._Utils.http.Progress` snapshot describing the
            batch throughput and latency (including the estimated seconds per
            1,000 requests).
        timeout : float | None, optional
            Timeout shared across the dispatched requests (default: ``10.0``).

        Returns
        -------
        list[dict[str, Any]]
            Parsed JSON payloads for successful responses in completion order.
        list[tuple[BatchRequest, Exception]]
            Requests that raised exceptions alongside their originating
            descriptors.
        """
        results: List[Dict[str, Any]] = []
        batch_requests: List[BatchRequest] = []

        for request in requests:
            payload = request.to_json()
            params = {
                "activations": str(request.activations).lower(),
                "synchronous-validation": str(request.synchronous_validation).lower(),
            }

            def _make_callback(target: "Segmentation") -> Callable[[Response], None]:
                def _callback(response: Response) -> None:
                    results.append(response.json())

                return _callback

            batch_requests.append(
                BatchRequest(
                    method="POST",
                    url=_API_ENDPOINT,
                    params=params,
                    json=payload,
                    timeout=timeout,
                    callback=_make_callback(request),
                )
            )

        _, errors = process_batch(
            batch_requests,
            api_key=api_key,
            max_workers=max_workers,
            progress_callback=progress_callback,
        )
        return results, errors
