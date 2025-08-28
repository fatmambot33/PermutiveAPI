"""Segment management for the Permutive API."""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Type, Union
from dataclasses import dataclass, field
from datetime import datetime, timezone

from PermutiveAPI.Audience import _API_ENDPOINT
from PermutiveAPI.Utils import RequestHelper, JSONSerializable

_API_PAYLOAD = ["name", "code", "description", "cpm", "categories"]


@dataclass
class Segment(JSONSerializable):
    """Represent a segment in the Permutive API.

    Attributes
    ----------
    code : str
        The code of the segment.
    name : str
        The name of the segment.
    import_id : str
        The import ID of the segment.
    id : Optional[str]
        The ID of the segment.
    description : Optional[str]
        The description of the segment.
    cpm : Optional[float]
        The cost per mille of the segment.
    categories : Optional[List[str]]
        Categories associated with the segment.
    created_at : Optional[datetime]
        When the segment was created.
    updated_at : Optional[datetime]
        When the segment was last updated.
    """

    code: str
    name: str
    import_id: str
    id: Optional[str] = None
    description: Optional[str] = None
    cpm: Optional[float] = 0.0
    categories: Optional[List[str]] = None
    created_at: Optional[datetime] = field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )
    updated_at: Optional[datetime] = field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )

    def create(self, api_key: str):
        """Create a new segment using the provided private key.

        Parameters
        ----------
        api_key : str
            The private key used for authentication.

        Raises
        ------
        ValueError
            If the segment creation fails.
        """
        logging.debug(f"SegmentAPI::create_segment::{self.import_id}::{self.name}")
        url = f"{_API_ENDPOINT}/{self.import_id}/segments"
        response = RequestHelper.post_static(
            api_key=api_key,
            url=url,
            data=RequestHelper.to_payload_static(
                dataclass_obj=self, api_payload=_API_PAYLOAD
            ),
        )
        if not response:
            raise ValueError("Unable to create_segment")

        new_segment = Segment.from_json(response.json())
        if isinstance(new_segment, Segment):
            self.__dict__.update(new_segment.__dict__)

    def update(self, api_key: str):
        """Update the segment using the provided private key.

        Parameters
        ----------
        api_key : str
            The private key used for authentication.

        Raises
        ------
        ValueError
            If the segment update fails.
        """
        logging.debug(f"SegmentAPI::update_segment::{self.import_id}::{self.name}")
        url = f"{_API_ENDPOINT}/{self.import_id}/segments/{self.id}"
        response = RequestHelper.patch_static(
            api_key=api_key,
            url=url,
            data=RequestHelper.to_payload_static(
                dataclass_obj=self, api_payload=_API_PAYLOAD
            ),
        )
        if not response:
            raise ValueError("Unable to update_segment")

        updated_segment = Segment.from_json(response.json())
        if isinstance(updated_segment, Segment):
            self.__dict__.update(updated_segment.__dict__)

    def delete(self, api_key: str) -> bool:
        """Delete a segment using the provided private key.

        Parameters
        ----------
        api_key : str
            The private key used for authentication.

        Returns
        -------
        bool
            ``True`` if the segment was successfully deleted (status code
            204), otherwise ``False``.
        """
        logging.debug(f"SegmentAPI::delete_segment::{self.import_id}::{self.id}")
        url = f"{_API_ENDPOINT}/{self.import_id}/segments/{self.id}"
        response = RequestHelper.delete_static(api_key=api_key, url=url)
        if response is None:
            raise ValueError("Response is None")
        return response.status_code == 204

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
            The private key for authentication.

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
        response = RequestHelper.get_static(url=url, api_key=api_key)
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
            The private key for authentication.

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
        response = RequestHelper.get_static(url=url, api_key=api_key)
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
            The private key used for authentication.

        Returns
        -------
        SegmentList
            A list of Segment objects retrieved from the API.

        Raises
        ------
        requests.exceptions.RequestException
            If an error occurs while making the API request.
        """
        logging.debug(f"SegmentAPI::list")

        base_url = f"{_API_ENDPOINT}/{import_id}/segments"
        all_segments = []
        next_token = None

        while True:
            # Construct the URL with the pagination token
            url = (
                f"{base_url}?pagination_token={next_token}" if next_token else base_url
            )
            response = RequestHelper.get_static(api_key=api_key, url=url)
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


class SegmentList(List[Segment], JSONSerializable):
    """Custom list that holds Segment objects and provides caching and serialization."""

    @classmethod
    def from_json(
        cls: Type["SegmentList"],
        data: Union[dict, List[dict], str, Path],
    ) -> "SegmentList":
        """Deserialize a list of segments from various JSON representations."""
        if isinstance(data, dict):
            raise TypeError(
                f"Cannot create a {cls.__name__} from a dictionary. Use from_json on the Segment class for single objects."
            )
        if isinstance(data, (str, Path)):
            try:
                if isinstance(data, Path):
                    content = data.read_text(encoding="utf-8")
                else:
                    content = data
                loaded_data = json.loads(content)
                if not isinstance(loaded_data, list):
                    raise TypeError(
                        f"JSON content from {type(data).__name__} did not decode to a list."
                    )
                data = loaded_data
            except Exception as e:
                raise TypeError(f"Failed to parse JSON from input: {e}")

        if isinstance(data, list):
            return cls([Segment.from_json(item) for item in data])

        raise TypeError(
            f"`from_json()` expected a list of dicts, JSON string, or Path, but got {type(data).__name__}"
        )

    def __init__(self, items_list: Optional[List[Segment]] = None):
        """Initialize the SegmentList with an optional list of Segment objects.

        Parameters
        ----------
        items_list : Optional[List[Segment]], optional
            Segment objects to initialize with. Defaults to None.
        """
        super().__init__(items_list if items_list is not None else [])
        self.rebuild_cache()

    def rebuild_cache(self):
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
            self.rebuild_cache()
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
            self.rebuild_cache()
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
            self.rebuild_cache()
        return self._code_dictionary_cache
