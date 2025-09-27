"""Segmentation helpers for the Permutive API.

This module wraps the CCS segmentation endpoint, allowing you to submit
arbitrary events for evaluation and retrieve the computed segment
membership in a structured way.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from requests import Response

from PermutiveAPI._Utils import http
from PermutiveAPI._Utils.http import BatchRequest, Progress, process_batch
from PermutiveAPI._Utils.json import JSONSerializable

_API_VERSION = "v1"
_API_ENDPOINT = f"https://api.permutive.app/ccs/{_API_VERSION}/segmentation"
_API_PAYLOAD = ["events", "user_id"]


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
    batch_send(requests, api_key, max_workers=None, progress_callback=None, activations=None,
               synchronous_validation=None, timeout=10.0)
        Dispatch multiple segmentation requests concurrently using the shared batch runner.

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
        return http.to_payload(self, _API_PAYLOAD)

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

    @classmethod
    def batch_send(
        cls,
        requests: Iterable["Segmentation"],
        *,
        api_key: str,
        max_workers: Optional[int] = None,
        progress_callback: Optional[Callable[[Progress], None]] = None,
        activations: Optional[bool] = None,
        synchronous_validation: Optional[bool] = None,
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
        activations : bool | None, optional
            Override for the activations query parameter applied to every
            request (default: ``None`` to use each instance's value).
        synchronous_validation : bool | None, optional
            Override for synchronous validation applied to every request
            (default: ``None`` to use each instance's value).
        timeout : float | None, optional
            Timeout shared across the dispatched requests (default: ``10.0``).

        Returns
        -------
        list[dict[str, Any]]
            Parsed JSON payloads for successful responses in completion order.
        list[tuple[BatchRequest, Exception]]
            Requests that raised exceptions alongside their originating
            descriptors.

        Notes
        -----
        This helper delegates work to :func:`PermutiveAPI._Utils.http.process_batch`.

        Examples
        --------
        >>> events = [
        ...     Event(
        ...         name="SlotViewable",
        ...         time="2025-07-01T15:39:11.594Z",
        ...         session_id="session-1",
        ...         view_id="view-1",
        ...         properties={"campaign_id": "3747123491"},
        ...     )
        ... ]
        >>> requests = [
        ...     Segmentation(user_id="user-1", events=events),
        ...     Segmentation(user_id="user-2", events=events),
        ... ]
        >>> responses, failures = Segmentation.batch_send(
        ...     requests,
        ...     api_key="test-key",
        ... )  # doctest: +SKIP
        >>> len(responses)  # doctest: +SKIP
        2
        >>> failures  # doctest: +SKIP
        []
        >>> def on_progress(progress):
        ...     avg = progress.average_per_thousand_seconds
        ...     avg_display = f"{avg:.2f}s" if avg is not None else "n/a"
        ...     print(
        ...         f"{progress.completed}/{progress.total} (errors: {progress.errors}) "
        ...         f"avg/1000: {avg_display}"
        ...     )
        >>> _responses, _failures = Segmentation.batch_send(
        ...     requests,
        ...     api_key="test-key",
        ...     activations=True,
        ...     synchronous_validation=True,
        ...     max_workers=4,
        ...     progress_callback=on_progress,
        ... )  # doctest: +SKIP
        """
        results: List[Dict[str, Any]] = []
        batch_requests: List[BatchRequest] = []

        for request in requests:
            params = cls._build_params(
                activations if activations is not None else request.activations,
                (
                    synchronous_validation
                    if synchronous_validation is not None
                    else request.synchronous_validation
                ),
            )
            payload = request.to_json()

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

    @staticmethod
    def _build_params(
        activations: bool, synchronous_validation: bool
    ) -> Dict[str, str]:
        """Return query parameters for the segmentation endpoint."""
        return {
            "activations": str(activations).lower(),
            "synchronous-validation": str(synchronous_validation).lower(),
        }
