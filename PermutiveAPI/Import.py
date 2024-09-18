import logging
from typing import Dict, List, Optional
from dataclasses import dataclass, field, asdict
from datetime import datetime
import json
from collections import defaultdict


from .Utils import RequestHelper, FileHelper


_API_VERSION = "v1"
_API_ENDPOINT = f'https://api.permutive.app/audience-api/{_API_VERSION}/imports'
_API_PAYLOAD = ['name', 'code', 'description', 'cpm', 'categories']


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
        logging.debug(f"{datetime.now()}::AudienceAPI::get_import::{id}")
        url = f"{_API_ENDPOINT}/{id}"
        response = RequestHelper.getRequest_static(url=url,
                                                   privateKey=privateKey)
        if not response:
            raise ValueError('Unable to get_import')
        return Import(**response.json())

    @staticmethod
    def list(privateKey: str) -> List['Import']:
        logging.debug(f"{datetime.now()}::AudienceAPI::list_imports")
        url = _API_ENDPOINT
        response = RequestHelper.getRequest_static(
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
            json.dump(asdict(self), f,
                      ensure_ascii=False, indent=4, default=FileHelper.json_default)

    @staticmethod
    def from_json(filepath: str) -> 'Import':
        with open(file=filepath, mode='r') as json_file:
            return Import(**json.load(json_file))


class ImportList(List[Import]):
    def __init__(self, imports: Optional[List[Import]] = None):
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


@dataclass
class Segment():
    """
    Dataclass for the Segment entity in the Permutive ecosystem.
    """

    code: str
    name: str
    import_id: str
    id: Optional[str] = None
    description: Optional[str] = None
    cpm: Optional[float] = 0.0
    categories: Optional[List[str]] = None
    updated_at: Optional[datetime] = field(default_factory=datetime.now)

    def create(self, privateKey: str):
        """
        Creates a new segment

        :return: The created Segment.
        """
        logging.debug(
            f"SegmentAPI::create_segment::{self.import_id}::{self.name}")
        url = f"{_API_ENDPOINT}/{self.import_id}/segments"
        response = RequestHelper.postRequest_static(privateKey=privateKey,
                                                    url=url,
                                                    data=RequestHelper.to_payload_static(dataclass_obj=self,
                                                                                         api_payload=_API_PAYLOAD))
        if not response:
            raise ValueError('Unable to create_segment')

        self = Segment(**response.json())

    def update(self, privateKey: str):
        """
        PATCH
        https://api.permutive.app/audience-api/v1/imports/{importId}/segments/{segmentId}
        Updates a segment for an import. The segment is identified by its globally unique public ID.
        https://developer.permutive.com/reference/patchimportsimportidsegmentssegmentid
        :param segment: SegmentAPI.Import.Segment to update
        :return: The updated Segment.
        """

        logging.debug(
            f"SegmentAPI::update_segment::{self.import_id}::{self.name}")
        url = f"{_API_ENDPOINT}/{self.import_id}/segments/{self.id}"
        response = RequestHelper.patchRequest_static(privateKey=privateKey,
                                                     url=url,
                                                     data=RequestHelper.to_payload_static(dataclass_obj=self,
                                                                                          api_payload=_API_PAYLOAD))
        if not response:
            raise ValueError('Unable to update_segment')
        self = Segment(**response.json())

    def delete(self, privateKey: str) -> bool:
        """
        Deletes a specific segment by its id.

        :param import_id: ID of the import.
        :param segment_id: ID of the segment.
        :return: True if deletion was successful, otherwise False.
        """
        logging.debug(
            f"SegmentAPI::delete_segment::{self.import_id:}::{self.id}")
        url = f"{_API_ENDPOINT}/{self.import_id}/segments/{self.id}"
        response = RequestHelper.deleteRequest_static(privateKey=privateKey,
                                                      url=url)
        return response.status_code == 204

    @staticmethod
    def get(import_id: str,
            segment_id: str,
            privateKey: str) -> 'Segment':
        """
        Fetches a specific segment by its id.
        https://developer.permutive.com/reference/getimportsimportidsegmentssegmentid
        :param import_id: ID of the import.
        :param segment_id: UUID of the segment.
        :return: The requested Segment.
        """
        logging.debug(
            f"SegmentAPI::get_segment_by_id::{import_id}::{segment_id}")
        url = f"{_API_ENDPOINT}/{import_id}/segments/{segment_id}"
        response = RequestHelper.getRequest_static(privateKey,
                                                   url=url)
        if not response:
            raise ValueError('Unable to get_segment')
        return Segment(**response.json())

    @staticmethod
    def get_by_code(
        import_id: str,
        segment_code: str,
            privateKey: str) -> 'Segment':
        """
        Fetches a specific segment by its code.
        https://developer.permutive.com/reference/getimportsimportidsegmentscodesegmentcode
        :param import_id: ID of the import.
        :param segment_code: Public code of the segment.
        :return: The requested Segment.
        """
        logging.debug(
            f"SegmentAPI::get_segment_by_code::{import_id}::{segment_code}")
        url = f"{_API_ENDPOINT}/{import_id}/segments/code/{segment_code}"
        response = RequestHelper.getRequest_static(url=url, privateKey=privateKey
                                                   )
        if not response:
            raise ValueError('Unable to get_segment')
        return Segment(**response.json())

    @staticmethod
    def get_by_id(id: str, privateKey: str) -> 'Segment':
        """
        Fetches a specific Segment by its id.

        :param import_id: ID of the import.
        :return: The requested Segment.
        """
        logging.debug(f"{datetime.now()}::SegmentAPI::get_segment:{id}")
        url = f"{_API_ENDPOINT}/{id}"
        response = RequestHelper.getRequest_static(url=url,
                                                   privateKey=privateKey)
        if not response:
            raise ValueError('Unable to get_by_id')
        return Segment(**response.json())

    @staticmethod
    def list(import_id: str, privateKey: str) -> List['Segment']:
        """
        Fetches all imports from the API.

        :return: List of all imports.
        """
        logging.debug(f"{datetime.now()}::SegmentAPI::list")
        url = f"{_API_ENDPOINT}/{import_id}/segments"
        response = RequestHelper.getRequest_static(privateKey=privateKey,
                                                   url=url)
        segments = response.json()
        return [Segment(**element) for element in segments.get('elements', [])]

    def to_json(self, filepath: str):
        FileHelper.check_filepath(filepath)
        with open(file=filepath, mode='w', encoding='utf-8') as f:
            json.dump(self, f,
                      ensure_ascii=False, indent=4, default=FileHelper.json_default)

    @staticmethod
    def from_json(filepath: str) -> 'Segment':
        with open(file=filepath, mode='r') as json_file:
            return Segment(**json.load(json_file))


class SegmentList(List[Segment]):

    def __init__(self, segments: Optional[List[Segment]] = None):
        """Initializes the SegmentList with an optional list of Segment objects."""
        super().__init__(segments if segments is not None else [])
        self.rebuild_cache()

    def rebuild_cache(self):
        """Rebuilds all caches based on the current state of the list."""
        self._id_dictionary_cache = {
            segment.id: segment for segment in self if segment.id}
        self._name_dictionary_cache = {
            segment.name: segment for segment in self if segment.name}
        self._code_dictionary_cache = {
            segment.code: segment for segment in self if segment.name}

    @property
    def id_dictionary(self) -> Dict[str, Segment]:
        """Returns a dictionary of segments indexed by their IDs."""
        if not self._id_dictionary_cache:
            self.rebuild_cache()
        return self._id_dictionary_cache

    @property
    def name_dictionary(self) -> Dict[str, Segment]:
        """Returns a dictionary of segments indexed by their names."""
        if not self._name_dictionary_cache:
            self.rebuild_cache()
        return self._name_dictionary_cache

    @property
    def code_dictionary(self) -> Dict[str, Segment]:
        """Returns a dictionary of segments indexed by their codes."""
        if not self._code_dictionary_cache:
            self.rebuild_cache()
        return self._code_dictionary_cache
