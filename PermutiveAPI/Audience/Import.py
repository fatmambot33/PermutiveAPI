"""Import management for the Permutive API."""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, DefaultDict, TYPE_CHECKING, Type, Union
from dataclasses import dataclass, field

if TYPE_CHECKING:
    from PermutiveAPI.Audience.Segment import SegmentList
from datetime import datetime, timezone
from collections import defaultdict
from PermutiveAPI.Utils import RequestHelper, JSONSerializable
from PermutiveAPI.Audience import _API_ENDPOINT
from PermutiveAPI.Audience.Source import Source


@dataclass
class Import(JSONSerializable):
    """Represents an Import in the Permutive ecosystem.

    Attributes
    ----------
    id : str
        The ID of the import.
    name : str
        The name of the import.
    code : str
        The code of the import.
    relation : str
        The relation of the import.
    identifiers : List[str]
        A list of identifiers for the import.
    source : Source
        The source of the import.
    description : Optional[str]
        An optional description of the import.
    inheritance : Optional[str]
        An optional inheritance of the import.
    segments : Optional[SegmentList]
        An optional list of segments in the import.
    created_at : Optional[datetime]
        The timestamp of the creation.
    updated_at : Optional[datetime]
        The timestamp of the last update.
    """

    id: str
    name: str
    code: str
    relation: str
    identifiers: List[str]
    source: "Source"
    description: Optional[str] = None
    inheritance: Optional[str] = None
    segments: Optional["SegmentList"] = None
    created_at: Optional[datetime] = field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )
    updated_at: Optional[datetime] = field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )

    @classmethod
    def get_by_id(cls, id: str, api_key: str) -> "Import":
        """Fetch a specific import by its ID.

        Parameters
        ----------
        id : str
            ID of the import.
        api_key : str
            The API key for authentication.

        Returns
        -------
        Import
            The requested import.
        """
        logging.debug(f"AudienceAPI::get_import::{id}")
        url = f"{_API_ENDPOINT}/{id}"
        response = RequestHelper.get_static(url=url, api_key=api_key)
        if not response:
            raise ValueError("Unable to get_import")
        return cls.from_json(response.json())

    @classmethod
    def list(cls, api_key: str) -> "ImportList":
        """Retrieve a list of all imports.

        Parameters
        ----------
        api_key : str
            The API key for authentication.

        Returns
        -------
        ImportList
            A list of Import objects.
        """
        logging.debug(f"AudienceAPI::list_imports")
        url = _API_ENDPOINT
        response = RequestHelper.get_static(api_key=api_key, url=url)
        if response is None:
            raise ValueError("Response is None")
        imports = response.json()
        return ImportList.from_json(imports["items"])


class ImportList(List[Import], JSONSerializable):
    """A class representing a list of Import objects with additional functionality for caching and serialization."""

    @classmethod
    def from_json(
        cls: Type["ImportList"],
        data: Union[dict, List[dict], str, Path],
    ) -> "ImportList":
        """Deserialize a list of imports from various JSON representations."""
        if isinstance(data, dict):
            raise TypeError(
                f"Cannot create a {cls.__name__} from a dictionary. Use from_json on the Import class for single objects."
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
            # Special handling for 'source' which is a nested JSONSerializable
            def create_import(item):
                source_data = item.get("source")
                if source_data:
                    source_instance = Source.from_json(source_data)
                    item["source"] = source_instance
                return Import.from_json(item)

            return cls([create_import(item) for item in data])

        raise TypeError(
            f"`from_json()` expected a list of dicts, JSON string, or Path, but got {type(data).__name__}"
        )

    def __init__(self, items_list: Optional[List[Import]] = None):
        """Initialize the ImportList with optional items.

        Parameters
        ----------
        items_list : Optional[List[Import]], optional
            Import objects to initialize with. Defaults to None.
        """
        super().__init__(items_list if items_list is not None else [])

        self._rebuild_cache()

    def _rebuild_cache(self):
        """Rebuild all caches based on the current state of the list."""
        self._id_dictionary_cache: Dict[str, Import] = {}
        self._name_dictionary_cache: Dict[str, Import] = {}
        self._code_dictionary_cache: Dict[str, Import] = {}
        self._identifier_dictionary_cache: DefaultDict[str, ImportList] = defaultdict(
            ImportList
        )
        for _import in self:
            self._id_dictionary_cache[_import.id] = _import
            self._name_dictionary_cache[_import.name] = _import
            self._code_dictionary_cache[_import.code] = _import
            for identifier in _import.identifiers:
                self._identifier_dictionary_cache[identifier].append(_import)

    @property
    def id_dictionary(self) -> Dict[str, Import]:
        """Return a dictionary of imports indexed by their IDs.

        Returns
        -------
        Dict[str, Import]
            Mapping of import IDs to ``Import`` objects.
        """
        if not self._id_dictionary_cache:
            self._rebuild_cache()
        return self._id_dictionary_cache

    @property
    def name_dictionary(self) -> Dict[str, Import]:
        """Return a dictionary of imports indexed by their names.

        Returns
        -------
        Dict[str, Import]
            Mapping of import names to ``Import`` objects.
        """
        if not self._name_dictionary_cache:
            self._rebuild_cache()
        return self._name_dictionary_cache
    
    @property
    def code_dictionary(self) -> Dict[str, Import]:
        """Return a dictionary of imports indexed by their names.

        Returns
        -------
        Dict[str, Import]
            Mapping of import names to ``Import`` objects.
        """
        if not self._code_dictionary_cache:
            self._rebuild_cache()
        return self._code_dictionary_cache

    @property
    def identifier_dictionary(self) -> Dict[str, "ImportList"]:
        """Return a dictionary of imports indexed by their identifiers.

        Returns
        -------
        Dict[str, ImportList]
            Mapping of identifiers to lists of imports.
        """
        if not self._identifier_dictionary_cache:
            self._rebuild_cache()
        return self._identifier_dictionary_cache
