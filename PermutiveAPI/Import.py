"""Import management for the Permutive API."""

import logging
from typing import Dict, List, Optional, DefaultDict, TYPE_CHECKING
from dataclasses import dataclass

if TYPE_CHECKING:
    from PermutiveAPI.Segment import SegmentList
from datetime import datetime
from collections import defaultdict

from PermutiveAPI.Utils import RequestHelper, JSONSerializable
from PermutiveAPI.Source import Source


_API_VERSION = "v1"
_API_ENDPOINT = f'https://api.permutive.app/audience-api/{_API_VERSION}/imports'


@dataclass
class Import(JSONSerializable):
    """
    Represents an Import in the Permutive ecosystem.

    :param id: The ID of the import.
    :type id: str
    :param name: The name of the import.
    :type name: str
    :param code: The code of the import.
    :type code: str
    :param relation: The relation of the import.
    :type relation: str
    :param identifiers: A list of identifiers for the import.
    :type identifiers: List[str]
    :param source: The source of the import.
    :type source: Source
    :param description: An optional description of the import.
    :type description: Optional[str]
    :param inheritance: An optional inheritance of the import.
    :type inheritance: Optional[str]
    :param segments: An optional list of segments in the import.
    :type segments: Optional[SegmentList]
    :param updated_at: The timestamp of the last update.
    :type updated_at: Optional[datetime]
    """

    id: str
    name: str
    code: str
    relation: str
    identifiers: List[str]
    source: 'Source'
    description: Optional[str] = None
    inheritance: Optional[str] = None
    segments: Optional['SegmentList'] = None
    updated_at: Optional[datetime] = datetime.now()

    @classmethod
    def get_by_id(cls,
                  id: str,
                  api_key: str) -> 'Import':
        """Fetch a specific import by its ID.

        Args:
            id (str): ID of the import.
            api_key (str): The API key for authentication.

        Returns:
            Import: The requested import.
        """
        logging.debug(f"AudienceAPI::get_import::{id}")
        url = f"{_API_ENDPOINT}/{id}"
        response = RequestHelper.get_static(url=url,
                                            api_key=api_key)
        if not response:
            raise ValueError('Unable to get_import')
        return cls(**response.json())

    @classmethod
    def list(cls,
             api_key: str) -> 'ImportList':
        """Retrieve a list of all imports.

        Args:
            api_key (str): The API key for authentication.

        Returns:
            ImportList: A list of Import objects.
        """
        logging.debug(f"AudienceAPI::list_imports")
        url = _API_ENDPOINT
        response = RequestHelper.get_static(
            api_key=api_key, url=url)
        if response is None:
            raise ValueError("Response is None")
        imports = response.json()

        def create_import(item):
            source_data = item.get('source')
            if source_data:
                source_instance = Source.from_json(source_data)
                item['source'] = source_instance
            return cls(**item)

        return ImportList([create_import(item) for item in imports['items']])


class ImportList(List[Import],
                 JSONSerializable):
    """
    A class representing a list of Import objects with additional functionality for caching and serialization.

    Attributes:
        _id_dictionary_cache (Dict[str, Import]): A cache dictionary of imports indexed by their IDs.
        _name_dictionary_cache (Dict[str, Import]): A cache dictionary of imports indexed by their names.
        _identifier_dictionary_cache (defaultdict[ImportList]): A cache dictionary of imports indexed by their identifiers.

    Methods:
        __init__(imports: Optional[List[Import]] = None):
            Initialize the ImportList with an optional list of Import objects and rebuilds the cache.

        rebuild_cache():
            Rebuild all caches based on the current state of the list.

        id_dictionary() -> Dict[str, Import]:
            Return a dictionary of imports indexed by their IDs.

        name_dictionary() -> Dict[str, Import]:
            Return a dictionary of imports indexed by their names.

        identifier_dictionary() -> Dict[str, 'ImportList']:
            Return a dictionary of imports indexed by their identifiers.

        to_json() -> List[dict]:
            Serialize the ImportList to a list of dictionaries.

        from_json(cls, data: List[dict]) -> 'ImportList':
            Deserializes a list of dictionaries to an ImportList object.
    """

    def __init__(self, items_list: Optional[List[Import]] = None):
        """Initialize the ImportList with optional items.

        Args:
            items_list (Optional[List[Import]]): Import objects to initialize with.

        Returns:
            None
        """
        super().__init__(items_list if items_list is not None else [])
        self._id_dictionary_cache: Dict[str, Import] = {}
        self._name_dictionary_cache: Dict[str, Import] = {}
        self._identifier_dictionary_cache: DefaultDict[str, ImportList] = defaultdict(
            ImportList)
        self.rebuild_cache()

    def rebuild_cache(self):
        """Rebuild all caches based on the current state of the list.

        Returns:
            None
        """
        for _import in self:
            self._id_dictionary_cache[_import.id] = _import
            self._name_dictionary_cache[_import.name] = _import
            for identifier in _import.identifiers:
                self._identifier_dictionary_cache[identifier].append(_import)
        return self

    @property
    def id_dictionary(self) -> Dict[str, Import]:
        """Return a dictionary of imports indexed by their IDs.

        Returns:
            Dict[str, Import]: Mapping of import IDs to ``Import`` objects.
        """
        if not self._id_dictionary_cache:
            self.rebuild_cache()
        return self._id_dictionary_cache

    @property
    def name_dictionary(self) -> Dict[str, Import]:
        """Return a dictionary of imports indexed by their names.

        Returns:
            Dict[str, Import]: Mapping of import names to ``Import`` objects.
        """
        if not self._name_dictionary_cache:
            self.rebuild_cache()
        return self._name_dictionary_cache

    @property
    def identifier_dictionary(self) -> Dict[str, 'ImportList']:
        """Return a dictionary of imports indexed by their identifiers.

        Returns:
            Dict[str, ImportList]: Mapping of identifiers to lists of imports.
        """
        if not self._identifier_dictionary_cache:
            self.rebuild_cache()
        return self._identifier_dictionary_cache

    def to_list(self) -> List[Import]:
        """Return the list of imports.

        Returns:
            List[Import]: The underlying list of imports.
        """
        return list(self)
