"""Context segmentation helpers for the Permutive API.

This module wraps the Context API endpoint, allowing callers to submit a page
URL and associated page properties to retrieve contextual segment matches.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from requests import Response

from .utils import http
from .utils.http import BatchRequest, Progress, process_batch
from .utils.json import JSONSerializable

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
    ...         "title": "Sports News: Local Team Wins Championship",
    ...         "category": "sports",
    ...         "subcategory": "baseball",
    ...         "tags": ["news", "sports", "baseball"],
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

    @classmethod
    def batch_send(
        cls,
        context_segments: Iterable["ContextSegment"],
        *,
        api_key: str,
        max_workers: Optional[int] = None,
        progress_callback: Optional[Callable[[Progress], None]] = None,
    ) -> Tuple[List[Response], List[Tuple[BatchRequest, Exception]]]:
        """Identify multiple users concurrently.

        Parameters
        ----------
        identities : Iterable[Identity]
            Identity payloads to submit.
        api_key : str
            API key for authentication.
        max_workers : int | None, optional
            Maximum number of worker threads (default: ``None`` to defer to the
            shared batch runner's default).
        progress_callback : Callable[[Progress], None] | None, optional
            Invoked after each request completes. Receives a
            :class:`~PermutiveAPI._Utils.http.Progress` snapshot describing
            throughput (including the estimated seconds per 1,000 requests).

        Returns
        -------
        list[Response]
            Successful HTTP responses in completion order.
        list[tuple[BatchRequest, Exception]]
            Requests that raised exceptions alongside their originating
            descriptors.

        Notes
        -----
        This helper delegates work to :func:`PermutiveAPI._Utils.http.process_batch`.

        Examples
        --------
        >>> identities = [
        ...     Identity(user_id="user-1", aliases=[Alias(alias_type="crm", alias_id="123")]),
        ...     Identity(user_id="user-2", aliases=[Alias(alias_type="crm", alias_id="456")]),
        ... ]
        >>> responses, failures = Identity.batch_identify(identities, api_key="test-key")
        >>> len(responses)  # doctest: +SKIP
        2
        >>> failures  # doctest: +SKIP
        []
        >>> def on_progress(progress):
        ...     avg = progress.average_per_thousand_seconds
        ...     avg_display = f"{avg:.2f}s" if avg is not None else "n/a"
        ...     print(
        ...         f"{progress.completed}/{progress.total} "
        ...         f"(errors: {progress.errors}, avg/1000: {avg_display})"
        ...     )
        >>> _responses, _failures = Identity.batch_identify(
        ...     identities,
        ...     api_key="test-key",
        ...     max_workers=4,
        ...     progress_callback=on_progress,
        ... )  # doctest: +SKIP
        """
        batch_requests: List[BatchRequest] = []

        for context_segment in context_segments:
            payload = cls._request_helper.to_payload(context_segment, _API_PAYLOAD)
            batch_requests.append(
                BatchRequest(
                    method="POST",
                    url=_API_ENDPOINT,
                    json=payload,
                )
            )

        return process_batch(
            batch_requests,
            api_key=api_key,
            max_workers=max_workers,
            progress_callback=progress_callback,
        )
