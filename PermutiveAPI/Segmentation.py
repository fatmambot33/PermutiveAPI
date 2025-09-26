"""Segmentation helpers for the Permutive API.

This module wraps the CCS segmentation endpoint, allowing you to submit
arbitrary events for evaluation and retrieve the computed segment
membership in a structured way.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from PermutiveAPI._Utils import http
from PermutiveAPI._Utils.json import JSONSerializable

_API_VERSION = "v1"
_API_ENDPOINT = f"https://api.permutive.app/ccs/{_API_VERSION}/segmentation"


@dataclass
class Event(JSONSerializable[Dict[str, Any]]):
    """Represent a single event passed to the segmentation API.

    Parameters
    ----------
    name : str
        Event name to evaluate.
    time : str
        ISO-8601 timestamp describing when the event occurred.
    session_id : str
        Session identifier associated with the event.
    view_id : str
        View identifier correlating events within the same page view.
    properties : dict[str, Any], optional
        Arbitrary event properties forwarded to Permutive (default: ``{}``).

    Methods
    -------
    to_json()
        Convert the event into a JSON-serialisable dictionary.
    from_json(data)
        Create an event from a JSON dictionary.
    """

    name: str
    time: str
    session_id: str
    view_id: str
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Segmentation(JSONSerializable[Dict[str, Any]]):
    """Submit events to the Permutive segmentation API.

    Parameters
    ----------
    events : list[SegmentationEvent]
        Collection of events that describe the interaction history.
    user_id : str
        The user identifier to evaluate against configured segments.
    activations : bool, optional
        Whether to request activation results in the response (default: ``False``).
    synchronous_validation : bool, optional
        Whether to force synchronous payload validation (default: ``False``).

    Methods
    -------
    to_json()
        Convert the request into the JSON payload expected by the API.
    send(api_key, activations=None, synchronous_validation=None, timeout=10.0)
        Dispatch the request to the segmentation endpoint and return the API response.

    Examples
    --------
    >>> event = SegmentationEvent(
    ...     name="SlotViewable",
    ...     time="2025-07-01T15:39:11.594Z",
    ...     session_id="f19199e4-1654-4869-b740-703fd5bafb6f",
    ...     view_id="d30ccfc5-c621-4ac4-a282-9a30ac864c8a",
    ...     properties={"campaign_id": "3747123491"},
    ... )
    >>> request = SegmentationRequest(user_id="user-123", events=[event])
    >>> request.send(api_key="permutive-api-key")  # doctest: +SKIP
    {"segments": []}
    """

    _request_helper = http

    events: List[Event]
    user_id: str
    activations: bool = False
    synchronous_validation: bool = False

    def to_json(self) -> Dict[str, Any]:
        """Return the JSON payload accepted by the segmentation API."""
        return {
            "events": [event.to_json() for event in self.events],
            "user_id": self.user_id,
        }

    def send(
        self,
        api_key: str,
        *,
        activations: Optional[bool] = None,
        synchronous_validation: Optional[bool] = None,
        timeout: Optional[float] = 10.0,
    ) -> Dict[str, Any]:
        """Submit the request to the segmentation endpoint.

        Parameters
        ----------
        api_key : str
            API key used to authenticate with Permutive.
        activations : bool | None, optional
            Override for the activations query parameter (default: ``None`` -> use
            ``self.activations``).
        synchronous_validation : bool | None, optional
            Override for the synchronous validation flag (default: ``None`` -> use
            ``self.synchronous_validation``).
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
        params = self._build_params(
            activations if activations is not None else self.activations,
            (
                synchronous_validation
                if synchronous_validation is not None
                else self.synchronous_validation
            ),
        )
        response = self._request_helper.request(
            method="POST",
            api_key=api_key,
            url=_API_ENDPOINT,
            params=params,
            json=self.to_json(),
            timeout=timeout,
        )
        return response.json()

    @staticmethod
    def _build_params(
        activations: bool, synchronous_validation: bool
    ) -> Dict[str, str]:
        """Return query parameters for the segmentation endpoint."""
        return {
            "activations": str(activations).lower(),
            "synchronous-validation": str(synchronous_validation).lower(),
        }
