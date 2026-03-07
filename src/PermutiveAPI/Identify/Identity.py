"""User identification helpers for the Permutive API."""

import logging
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from dataclasses import dataclass


from . import _API_ENDPOINT
from .Alias import Alias
from requests import Response

from PermutiveAPI._Utils import http
from PermutiveAPI._Utils.http import BatchRequest, Progress, process_batch
from PermutiveAPI._Utils.json import JSONSerializable

_API_PAYLOAD = ["user_id", "aliases"]


@dataclass
class Identity(JSONSerializable[Dict[str, Any]]):
    """Dataclass for the Identity entity in the Permutive ecosystem.

    Parameters
    ----------
    user_id : str
        The user's ID.
    aliases : List[Alias]
        A list of aliases for the user.

    Methods
    -------
    identify(api_key)
        Identify a user in Permutive.
    batch_identify(identities, api_key, max_workers=None, progress_callback=None)
        Identify multiple users concurrently via the shared batch runner.
    """

    _request_helper = http

    user_id: str
    aliases: List[Alias]

    def identify(self, api_key: str) -> None:
        """Identify a user in Permutive.

        This method sends a POST request to the Permutive API to identify a user
        with the given aliases. Any exceptions raised during the request are
        propagated to the caller.

        Parameters
        ----------
        api_key : str
            The API key for authentication.

        Raises
        ------
        ValueError
            If the API call returns no response.
        PermutiveAPIError
            If the Permutive API responds with an error.
        """
        logging.debug(f"UserAPI::identify::{self.user_id}")

        url = f"{_API_ENDPOINT}"

        self._request_helper.post(
            api_key=api_key,
            url=url,
            data=self._request_helper.to_payload(self, _API_PAYLOAD),
        )

    @classmethod
    def batch_identify(
        cls,
        identities: Iterable["Identity"],
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

        for identity in identities:
            payload = cls._request_helper.to_payload(identity, _API_PAYLOAD)
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
