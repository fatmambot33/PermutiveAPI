import logging
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
import json
from collections import defaultdict
from collections.abc import Iterable


from .APIRequestHandler import APIRequestHandler
from .Segment import SegmentList
from .Utils import FileHelper

_API_VERSION = "v1"
_API_ENDPOINT = f'https://api.permutive.app/audience-api/{_API_VERSION}/imports'


@dataclass
class Source():
    """
    Dataclass for the Source entity in the Permutive ecosystem.
    """
    id: str
    state: Dict
    type: str
    bucket: Optional[str] = None
    permissions: Optional[Dict] = None
    phase: Optional[str] = None
    errors: Optional[List[str]] = None
    advertiser_name: Optional[str] = None
    organization_id: Optional[str] = None
    version: Optional[str] = None

    def to_json(self, filepath: str):
        FileHelper.check_filepath(filepath)
        with open(file=filepath, mode='w', encoding='utf-8') as f:
            json.dump(self, f,
                      ensure_ascii=False, indent=4, default=FileHelper.json_default)

    @staticmethod
    def from_json(filepath: str) -> 'Source':
        with open(file=filepath, mode='r') as json_file:
            return Source(**json.load(json_file))


@dataclass
class Import():
    """
    Dataclass for the Import in the Permutive ecosystem.
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

    @staticmethod
    def get_by_id(id: str,
                  privateKey: str) -> 'Import':
        """
        Fetches a specific import by its id.

        :param import_id: ID of the import.
        :return: The requested Importt.
        """
        logging.debug(f"AudienceAPI::get_import::{id}")
        url = f"{_API_ENDPOINT}/{id}"
        response = APIRequestHandler.getRequest_static(url=url,
                                                       privateKey=privateKey)
        if not response:
            raise ValueError('Unable to get_import')
        return Import(**response.json())

    @staticmethod
    def list(privateKey: str) -> List['Import']:
        logging.debug(f"AudienceAPI::list_imports")
        url = _API_ENDPOINT
        response = APIRequestHandler.getRequest_static(
            privateKey=privateKey, url=url)
        imports = response.json()

        def create_import(item):
            source_data = item.get('source')
            if source_data:
                source_instance = Source(**source_data)
                item['source'] = source_instance
            return Import(**item)

        return [create_import(item) for item in imports['items']]

    def to_json(self, filepath: str):
        FileHelper.check_filepath(filepath)
        with open(file=filepath, mode='w', encoding='utf-8') as f:
            json.dump(self, f,
                      ensure_ascii=False, indent=4, default=FileHelper.json_default)

    @staticmethod
    def from_json(filepath: str) -> 'Import':
        with open(file=filepath, mode='r') as json_file:
            return Import(**json.load(json_file))


@dataclass
class ImportList(List[Import]):
    # Cache for each dictionary to avoid rebuilding
    _id_dictionary_cache: Dict[str, Import] = field(
        default_factory=dict, init=False)
    _name_dictionary_cache: Dict[str, Import] = field(
        default_factory=dict, init=False)
    _identifier_dictionary_cache: Dict[str, 'ImportList'] = field(
        default_factory=dict, init=False)

    def __init__(self, imports: Optional[List[Import]] = None):
        """Initializes the ImportList with an optional list of Import objects."""
        super().__init__(imports if imports is not None else [])
        self.rebuild_cache()

    def rebuild_cache(self):
        """Rebuilds all caches based on the current state of the list."""
        self._id_dictionary_cache = {
            import_.id: import_ for import_ in self if import_.id}
        self._name_dictionary_cache = {
            import_.name: import_ for import_ in self if import_.name}
        self._identifier_dictionary_cache = defaultdict(ImportList)
        for import_ in self:
            for identifier in import_.identifiers:
                self._identifier_dictionary_cache[identifier].append(import_)

    @property
    def id_dictionary(self) -> Dict[str, Import]:
        """Returns a dictionary of imports indexed by their IDs."""
        if not self._id_dictionary_cache:
            self.rebuild_cache()
        return self._id_dictionary_cache

    @property
    def name_dictionary(self) -> Dict[str, Import]:
        """Returns a dictionary of imports indexed by their names."""
        if not self._name_dictionary_cache:
            self.rebuild_cache()
        return self._name_dictionary_cache

    @property
    def identifier_dictionary(self) -> Dict[str, 'ImportList']:
        """Returns a dictionary of imports indexed by their identifiers."""
        if not self._identifier_dictionary_cache:
            self.rebuild_cache()
        return self._identifier_dictionary_cache
