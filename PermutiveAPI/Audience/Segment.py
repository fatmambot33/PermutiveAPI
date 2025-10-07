"""Segment management for the Permutive API."""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)

from requests import Response
import pandas as pd

from . import _API_ENDPOINT
from PermutiveAPI._Utils import http
from PermutiveAPI._Utils.http import BatchRequest, Progress, process_batch
from PermutiveAPI._Utils.json import JSONSerializable, load_json_list

_API_PAYLOAD = ["name", "code", "description", "cpm", "categories"]


@dataclass
class Segment(JSONSerializable[Dict[str, Any]]):
    """Represent a segment in the Permutive API.

    Parameters
    ----------
    code : str
        The code of the segment.
    name : str
        The name of the segment.
    import_id : str
        The import ID of the segment.
    id : Optional[str], optional
        The ID of the segment (default: None).
    description : Optional[str], optional
        The description of the segment (default: None).
    cpm : Optional[float], optional
        The cost per mille of the segment (default: 0.0).
    categories : Optional[List[str]], optional
        Categories associated with the segment (default: None).
    created_at : Optional[datetime], optional
        When the segment was created (default: None).
    updated_at : Optional[datetime], optional
        When the segment was last updated (default: None).

    Methods
    -------
    create(api_key)
        Create a new segment.
    update(api_key)
        Update the segment.
    delete(api_key)
        Delete a segment.
    batch_create(segments, api_key, max_workers=None, progress_callback=None)
        Create multiple segments concurrently via the shared batch runner.
    batch_update(segments, api_key, max_workers=None, progress_callback=None)
        Update multiple segments concurrently via the shared batch runner.
    batch_delete(segments, api_key, max_workers=None, progress_callback=None)
        Delete multiple segments concurrently via the shared batch runner.
    get_by_code(import_id, segment_code, api_key)
        Retrieve a segment by its code.
    get_by_id(import_id, segment_id, api_key)
        Retrieve a segment by its ID.
    list(import_id, api_key)
        Retrieve a list of segments for a given import ID.
    """

    code: str
    _request_helper = http

    name: str
    import_id: str
    id: Optional[str] = None
    description: Optional[str] = None
    cpm: Optional[float] = 0.0
    categories: Optional[List[str]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def __post_init__(self) -> None:
        """Normalize timestamps for deterministic serialization.

        If one of ``created_at`` or ``updated_at`` is missing, it is copied
        from the other. If both are missing, they are initialized to the
        same current UTC timestamp. This avoids microsecond-level drift
        between the two fields.
        """
        if self.created_at is None and self.updated_at is None:
            now = datetime.now(tz=timezone.utc)
            self.created_at = now
            self.updated_at = now
        elif self.created_at is None:
            self.created_at = self.updated_at
        elif self.updated_at is None:
            self.updated_at = self.created_at

    def create(self, api_key: str) -> None:
        """Create a new segment using the provided API key.

        Parameters
        ----------
        api_key : str
            The API key used for authentication.

        Raises
        ------
        ValueError
            If the segment creation fails.
        """
        logging.debug(f"SegmentAPI::create_segment::{self.import_id}::{self.name}")
        url = f"{_API_ENDPOINT}/{self.import_id}/segments"
        response = self._request_helper.post(
            api_key=api_key,
            url=url,
            data=http.to_payload(dataclass_obj=self, api_payload=_API_PAYLOAD),
        )
        if not response:
            raise ValueError("Unable to create_segment")

        new_segment = Segment.from_json(response.json())
        if isinstance(new_segment, Segment):
            self.__dict__.update(new_segment.__dict__)

    def update(self, api_key: str) -> None:
        """Update the segment using the provided API key.

        Parameters
        ----------
        api_key : str
            The API key used for authentication.

        Raises
        ------
        ValueError
            If the segment update fails.
        """
        logging.debug(f"SegmentAPI::update_segment::{self.import_id}::{self.name}")
        url = f"{_API_ENDPOINT}/{self.import_id}/segments/{self.id}"
        response = self._request_helper.patch(
            api_key=api_key,
            url=url,
            data=http.to_payload(dataclass_obj=self, api_payload=_API_PAYLOAD),
        )
        if not response:
            raise ValueError("Unable to update_segment")

        updated_segment = Segment.from_json(response.json())
        if isinstance(updated_segment, Segment):
            self.__dict__.update(updated_segment.__dict__)

    def delete(self, api_key: str) -> None:
        """Delete a segment using the provided API key.

        Parameters
        ----------
        api_key : str
            The API key used for authentication.

        Raises
        ------
        ValueError
            If the segment deletion fails.
        """
        logging.debug(f"SegmentAPI::delete_segment::{self.import_id}::{self.id}")
        url = f"{_API_ENDPOINT}/{self.import_id}/segments/{self.id}"
        response = self._request_helper.delete(api_key=api_key, url=url)
        if response is None:
            raise ValueError("Response is None")

    @classmethod
    def batch_create(
        cls,
        segments: Iterable["Segment"],
        *,
        api_key: str,
        max_workers: Optional[int] = None,
        progress_callback: Optional[Callable[[Progress], None]] = None,
    ) -> Tuple[List[Response], List[Tuple[BatchRequest, Exception]]]:
        """Create multiple segments concurrently.

        Parameters
        ----------
        segments : Iterable[Segment]
            Segment instances to submit. Each instance is updated in-place with
            the API payload before the shared progress callback executes.
        api_key : str
            API key for authentication.
        max_workers : int | None, optional
            Maximum number of worker threads (default: ``None`` to defer to the
            shared batch runner's default).
        progress_callback : Callable[[Progress], None] | None, optional
            Invoked after each request completes. Receives a
            :class:`~PermutiveAPI._Utils.http.Progress` snapshot describing the
            batch throughput and latency (including the estimated seconds per
            1,000 requests).

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
        >>> segments = [
        ...     Segment(import_id="import-1", name="VIP", code="vip"),
        ...     Segment(import_id="import-1", name="Returning", code="returning"),
        ... ]
        >>> responses, failures = Segment.batch_create(
        ...     segments,
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
        >>> _responses, _failures = Segment.batch_create(
        ...     segments,
        ...     api_key="test-key",
        ...     max_workers=4,
        ...     progress_callback=on_progress,
        ... )  # doctest: +SKIP
        """
        batch_requests: List[BatchRequest] = []

        for segment in segments:
            url = f"{_API_ENDPOINT}/{segment.import_id}/segments"
            payload = http.to_payload(dataclass_obj=segment, api_payload=_API_PAYLOAD)

            def _make_callback(target: "Segment") -> Callable[[Response], None]:
                def _callback(response: Response) -> None:
                    created = cls.from_json(response.json())
                    if isinstance(created, cls):
                        target.__dict__.update(created.__dict__)

                return _callback

            batch_requests.append(
                BatchRequest(
                    method="POST",
                    url=url,
                    json=payload,
                    callback=_make_callback(segment),
                )
            )

        return process_batch(
            batch_requests,
            api_key=api_key,
            max_workers=max_workers,
            progress_callback=progress_callback,
        )

    @classmethod
    def batch_update(
        cls,
        segments: Iterable["Segment"],
        *,
        api_key: str,
        max_workers: Optional[int] = None,
        progress_callback: Optional[Callable[[Progress], None]] = None,
    ) -> Tuple[List[Response], List[Tuple[BatchRequest, Exception]]]:
        """Update multiple segments concurrently.

        Parameters
        ----------
        segments : Iterable[Segment]
            Segment instances to update. Each must define ``id`` and is updated
            in-place with the response payload before the progress callback
            executes.
        api_key : str
            API key for authentication.
        max_workers : int | None, optional
            Maximum number of worker threads (default: ``None`` to defer to the
            shared batch runner's default).
        progress_callback : Callable[[Progress], None] | None, optional
            Invoked after each request completes. Receives a
            :class:`~PermutiveAPI._Utils.http.Progress` snapshot describing the
            batch throughput and latency (including the estimated seconds per
            1,000 requests).

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
        >>> segments = [
        ...     Segment(import_id="import-1", id="seg-1", name="VIP", code="vip"),
        ...     Segment(import_id="import-1", id="seg-2", name="Returning", code="returning"),
        ... ]
        >>> responses, failures = Segment.batch_update(
        ...     segments,
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
        >>> _responses, _failures = Segment.batch_update(
        ...     segments,
        ...     api_key="test-key",
        ...     max_workers=4,
        ...     progress_callback=on_progress,
        ... )  # doctest: +SKIP
        """
        batch_requests: List[BatchRequest] = []

        for segment in segments:
            if not segment.id:
                raise ValueError("Segment ID must be specified for update.")
            url = f"{_API_ENDPOINT}/{segment.import_id}/segments/{segment.id}"
            payload = http.to_payload(dataclass_obj=segment, api_payload=_API_PAYLOAD)

            def _make_callback(target: "Segment") -> Callable[[Response], None]:
                def _callback(response: Response) -> None:
                    updated = cls.from_json(response.json())
                    if isinstance(updated, cls):
                        target.__dict__.update(updated.__dict__)

                return _callback

            batch_requests.append(
                BatchRequest(
                    method="PATCH",
                    url=url,
                    json=payload,
                    callback=_make_callback(segment),
                )
            )

        return process_batch(
            batch_requests,
            api_key=api_key,
            max_workers=max_workers,
            progress_callback=progress_callback,
        )

    @classmethod
    def batch_delete(
        cls,
        segments: Iterable["Segment"],
        *,
        api_key: str,
        max_workers: Optional[int] = None,
        progress_callback: Optional[Callable[[Progress], None]] = None,
    ) -> Tuple[List[Response], List[Tuple[BatchRequest, Exception]]]:
        """Delete multiple segments concurrently.

        Parameters
        ----------
        segments : Iterable[Segment]
            Segment instances to delete. Each must define ``id``.
        api_key : str
            API key for authentication.
        max_workers : int | None, optional
            Maximum number of worker threads (default: ``None``).
        progress_callback : Callable[[Progress], None] | None, optional
            Invoked after each request completes. Receives a
            :class:`~PermutiveAPI._Utils.http.Progress` snapshot describing the
            batch throughput and latency (including the estimated seconds per
            1,000 requests).

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
        >>> segments = [
        ...     Segment(import_id="import-1", id="seg-1", name="VIP"),
        ...     Segment(import_id="import-1", id="seg-2", name="Returning"),
        ... ]
        >>> responses, failures = Segment.batch_delete(
        ...     segments,
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
        >>> _responses, _failures = Segment.batch_delete(
        ...     segments,
        ...     api_key="test-key",
        ...     max_workers=4,
        ...     progress_callback=on_progress,
        ... )  # doctest: +SKIP
        """
        batch_requests: List[BatchRequest] = []

        for segment in segments:
            if not segment.id:
                raise ValueError("Segment ID must be specified for deletion.")
            url = f"{_API_ENDPOINT}/{segment.import_id}/segments/{segment.id}"
            batch_requests.append(BatchRequest(method="DELETE", url=url))

        return process_batch(
            batch_requests,
            api_key=api_key,
            max_workers=max_workers,
            progress_callback=progress_callback,
        )

    @staticmethod
    def get_by_code(import_id: str, segment_code: str, api_key: str) -> "Segment":
        """Retrieve a segment by its code.

        Parameters
        ----------
        import_id : str
            The ID of the import.
        segment_code : str
            The code of the segment to retrieve.
        api_key : str
            The API key for authentication.

        Returns
        -------
        Segment
            The segment object retrieved by the given code.

        Raises
        ------
        ValueError
            If the segment cannot be retrieved.
        """
        logging.debug(f"SegmentAPI::get_segment_by_code::{import_id}::{segment_code}")
        url = f"{_API_ENDPOINT}/{import_id}/segments/code/{segment_code}"
        response = Segment._request_helper.get(url=url, api_key=api_key)
        if not response:
            raise ValueError("Unable to get_segment")
        return Segment.from_json(response.json())

    @staticmethod
    def get_by_id(import_id: str, segment_id: str, api_key: str) -> "Segment":
        """Retrieve a segment by its ID.

        Parameters
        ----------
        import_id : str
            The ID of the import.
        segment_id : str
            The ID of the segment to retrieve.
        api_key : str
            The API key for authentication.

        Returns
        -------
        Segment
            The segment object retrieved by the given ID.

        Raises
        ------
        ValueError
            If the segment cannot be retrieved.
        """
        logging.debug(f"SegmentAPI::get_segment_by_id::{import_id}::{segment_id}")
        url = f"{_API_ENDPOINT}/{import_id}/segments/{segment_id}"
        response = Segment._request_helper.get(url=url, api_key=api_key)
        if not response:
            raise ValueError("Unable to get_by_id")
        return Segment.from_json(response.json())

    @staticmethod
    def list(import_id: str, api_key: str) -> "SegmentList":
        """Retrieve a list of segments for a given import ID.

        Parameters
        ----------
        import_id : str
            The ID of the import to retrieve segments for.
        api_key : str
            The API key for authentication.

        Returns
        -------
        SegmentList
            A list of Segment objects retrieved from the API.

        Raises
        ------
        ValueError
            If the segment list cannot be fetched.
        PermutiveAPIError
            If an error occurs while making the API request.
        """
        logging.debug(f"SegmentAPI::list")

        base_url = f"{_API_ENDPOINT}/{import_id}/segments"
        all_segments = []
        next_token = None

        while True:
            params = {}
            if next_token:
                params["pagination_token"] = next_token

            response = Segment._request_helper.get(api_key, base_url, params=params)
            if response is None:
                raise ValueError("Response is None")
            data = response.json()

            # Extract elements and add them to the list
            all_segments.extend(data.get("elements", []))

            # Check for next_token in the pagination metadata
            next_token = data.get("pagination", {}).get("next_token")

            if not next_token:
                break  # Stop when there are no more pages

        return SegmentList.from_json(all_segments)


class SegmentList(List[Segment], JSONSerializable[List[Any]]):
    """Custom list that holds Segment objects and provides caching and serialization.

    Methods
    -------
    from_json(data)
        Deserialize a list of segments from various JSON representations.
    id_dictionary()
        Return a dictionary of segments indexed by their IDs.
    name_dictionary()
        Return a dictionary of segments indexed by their names.
    code_dictionary()
        Return a dictionary of segments indexed by their codes.
    to_pd_dataframe()
        Convert the segment list into a pandas ``DataFrame``.
    """

    @classmethod
    def from_json(
        cls: Type["SegmentList"],
        data: Union[dict, List[dict], str, Path],
    ) -> "SegmentList":
        """Deserialize a list of segments from various JSON representations.

        Parameters
        ----------
        data : Union[dict, List[dict], str, Path]
            The JSON data to deserialize. It can be a dictionary, a list of
            dictionaries, a JSON string, or a path to a JSON file.

        Returns
        -------
        SegmentList
            A `SegmentList` instance created from the provided JSON data.
        """
        data_list = load_json_list(data, cls.__name__, "Segment")
        return cls([Segment.from_json(item) for item in data_list])

    def __init__(self, items_list: Optional[List[Segment]] = None):
        """Initialize the SegmentList with an optional list of Segment objects.

        Parameters
        ----------
        items_list : Optional[List[Segment]], optional
            Segment objects to initialize with (default: None).
        """
        super().__init__(items_list if items_list is not None else [])
        self._id_dictionary_cache: Dict[str, Segment] = {}
        self._name_dictionary_cache: Dict[str, Segment] = {}
        self._code_dictionary_cache: Dict[str, Segment] = {}
        self._refresh_cache()

    def _refresh_cache(self) -> None:
        """Rebuild all caches based on the current state of the list."""
        self._id_dictionary_cache = {
            segment.id: segment for segment in self if segment.id
        }
        self._name_dictionary_cache = {
            segment.name: segment for segment in self if segment.name
        }
        self._code_dictionary_cache = {
            segment.code: segment for segment in self if segment.code
        }

    @property
    def id_dictionary(self) -> Dict[str, Segment]:
        """Return a dictionary of segments indexed by their IDs.

        Returns
        -------
        Dict[str, Segment]
            A dictionary mapping segment IDs to Segment objects.
        """
        if not self._id_dictionary_cache:
            self._refresh_cache()
        return self._id_dictionary_cache

    @property
    def name_dictionary(self) -> Dict[str, Segment]:
        """Return a dictionary of segments indexed by their names.

        Returns
        -------
        Dict[str, Segment]
            A dictionary where the keys are segment names and the values are Segment objects.
        """
        if not self._name_dictionary_cache:
            self._refresh_cache()
        return self._name_dictionary_cache

    @property
    def code_dictionary(self) -> Dict[str, Segment]:
        """Return a dictionary of segments indexed by their codes.

        Returns
        -------
        Dict[str, Segment]
            A dictionary where the keys are segment codes and the values are Segment objects.
        """
        if not self._code_dictionary_cache:
            self._refresh_cache()
        return self._code_dictionary_cache

    def to_pd_dataframe(self) -> "pd.DataFrame":
        """Convert the segment list into a pandas ``DataFrame``.

        Returns
        -------
        pandas.DataFrame
            A dataframe containing one row per segment with serialized fields.
        """
        records = [cast(Dict[str, Any], segment.to_json()) for segment in self]
        return pd.DataFrame(records)
