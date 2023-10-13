from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional
import logging
from .APIRequestHandler import APIRequestHandler
from .CohortAPI import CohortAPI
from .Utils import ListHelper, FileHelper

AUDIENCE_API_VERSION = 'v1'
AUDIENCE_API_ENDPOINT = f'https://api.permutive.app/audience-api/{AUDIENCE_API_VERSION}/imports'


class AudienceAPI:

    """
        AudienceAPI class
        This class is responsible for interacting with the audience end points of the permutive API.
    """

    def __init__(self, api_key: str):
        logging.info(f"AudienceAPI::__init__")
        self.__api_key = api_key

    @dataclass
    class Import:
        """
        Dataclass for the Provider entity in the Permutive ecosystem.
        """
        id: str
        name: str
        code: str
        relation: str
        identifiers: List[str]
        source: 'Source'
        description: Optional[Dict] = None
        source: Optional['Source'] = None
        inheritance: Optional[str] = None
        segments: Optional[List['Segment']] = None
        updated_at: Optional[datetime] = datetime.now()

        def to_file(self, filepath: str):
            FileHelper.save_to_json(self, filepath=filepath)

        @staticmethod
        def from_file(filepath: str):
            jsonObj = FileHelper.read_json(filepath=filepath)
            return AudienceAPI.Import(**jsonObj)

        @dataclass
        class Source:
            """
            Dataclass for the Source entity in the Permutive ecosystem.
            """
            id: str
            state: Dict
            bucket: str
            permissions: Dict
            phase: str
            type: str

            def to_file(self, filepath: str):
                FileHelper.save_to_json(self, filepath=filepath)

            @staticmethod
            def from_file(filepath: str):
                jsonObj = FileHelper.read_json(filepath=filepath)
                return AudienceAPI.Import.Source(**jsonObj)

        @dataclass
        class Segment:
            """
            Dataclass for the Segment entity in the Permutive ecosystem.
            """
            id: str
            code: str
            name: str
            import_id: Optional[str] = None
            description: Optional[str] = None
            cpm: Optional[float] = 0.0
            categories: Optional[List[str]] = None
            updated_at: Optional[datetime] = datetime.now()

            def to_file(self, filepath: str):
                FileHelper.save_to_json(self, filepath=filepath)

            @staticmethod
            def from_file(filepath: str):
                jsonObj = FileHelper.read_json(filepath=filepath)
                return AudienceAPI.Import.Segment(**jsonObj)

    def list_imports(self) -> List[Import]:
        """
        Fetches all providers from the API.

        :return: List of all providers.
        """
        logging.info(f"AudienceAPI::list_imports")
        url = f"{AUDIENCE_API_ENDPOINT}?k={self.__api_key}"
        response = APIRequestHandler.get(url=url)
        provider_data = response.json()
        return [AudienceAPI.Import(**provider) for provider in provider_data['items']]

    def list_segments(self, import_id: str) -> List[Import.Segment]:
        """
        Fetches all segments for a specific data provider.

        :param data_provider_id: ID of the data provider.
        :return: List of all segments.
        """
        logging.info(f"AudienceAPI::list_segments::{import_id}")
        url = f"{AUDIENCE_API_ENDPOINT}/{import_id}/segments?k={self.__api_key}"
        response = APIRequestHandler.get(url=url)
        segment_data = response.json()

        return [AudienceAPI.Import.Segment(**segment) for segment in segment_data['elements']]

    def get_segment(self,  import_id: str, segment_id: str) -> Import.Segment:
        """
        Fetches a specific segment by its id.

        :param data_provider_id: ID of the data provider.
        :param segment_id: ID of the segment.
        :return: The requested Segment.
        """
        logging.info(f"AudienceAPI::get_segment::{import_id}::{segment_id}")
        url = f"{AUDIENCE_API_ENDPOINT}/{import_id}/segments/{segment_id}?k={self.__api_key}"
        response = APIRequestHandler.get(url=url)
        return AudienceAPI.Import.Segment(**response.json())

    def create_segment(self, segment: Import.Segment) -> Import.Segment:
        """
        Creates a new segment for a specific import_id.

        :return: The created Segment.
        """
        if segment.import_id is None:
            raise ValueError('import_idmust be specified')
        if segment.name is None:
            raise ValueError('name must be specified')
        logging.info(
            f"AudienceAPI::create_segment::{segment.import_id}::{segment.name}")
        url = f"{AUDIENCE_API_ENDPOINT}/{segment.import_idd}/segments?k={self.__api_key}"
        payload = ['name', 'code', 'description', 'cpm', 'categories']
        response = APIRequestHandler.post(
            url=url, data=segment.to_payload(payload))
        return AudienceAPI.Import.Segment(**response.json())

    def update_segment(self, segment: Import.Segment) -> Optional[Import.Segment]:
        """
        Updates a specific segment by its id.

        :param data_provider_id: ID of the data provider.
        :param segment_id: ID of the segment.
        :param segment_data: Updated data of the segment.
        :return: The updated Segment.
        """
        if segment.import_id is None:
            raise ValueError('data_provider_id must be specified')
        if segment.id is None:
            raise ValueError('id must be specified')
        logging.info(
            f"AudienceAPI::update_segment::{segment.import_id}::{segment.name}")
        url = f"{AUDIENCE_API_ENDPOINT}/{segment.import_id}/segments/{segment.id}?k={self.__api_key}"
        payload = ['name', 'code', 'description', 'cpm', 'categories']
        response = APIRequestHandler.patch(
            url=url,  data=APIRequestHandler.to_payload(segment, payload))
        if response is None:
            return response
        return AudienceAPI.Import.Segment(**response.json())

    def delete_segments(self, import_id: str, segment_id: str) -> bool:
        """
        Deletes a specific segment by its id.

        :param data_provider_id: ID of the data provider.
        :param segment_id: ID of the segment.
        :return: True if deletion was successful, otherwise False.
        """
        logging.info(
            f"AudienceAPI::delete_segment::{import_id:}::{segment_id}")
        url = f"{AUDIENCE_API_ENDPOINT}/{import_id}/segments/{segment_id}?k={self.__api_key}"
        response = APIRequestHandler.delete(url=url)
        return response.status_code == 204

    def create_cohorts(self, import_id: str):
        logging.info(
            f"AudienceAPI::create_cohorts::{import_id:}")
        imports = self.list_imports()
        _import = None
        for _import_ in imports:
            if _import_.id == import_id:
                _import = _import_

        if _import is None:
            raise ValueError(f'import {import_id} does not exist')

        import_segments = self.list_segments(import_id=import_id)
        cohort_api = CohortAPI(self.__api_key)
        for import_segment in import_segments:
            logging.info(
                f"AudienceAPI::create_cohorts::{import_id:}::{import_segment.name}")
            segment_cohort = CohortAPI.Cohort(
                name=import_segment.name, tags=[_import.name, '#automatic', '#imports'])
            segment_cohort.query = {"in_second_party_segment": {
                "provider": _import.code,
                "segment": import_segment.code
            }}
            segment_cohort.description = f'{_import.name} ({_import.id}) : {import_segment.code} : {import_segment.name} ({import_segment.id})'
            cohort = cohort_api.get_by_name(segment_cohort.name)

            if cohort is None:
                cohort_api.create(segment_cohort)
            else:
                segment_cohort.id = cohort.id
                segment_cohort.code = cohort.code
                segment_cohort.tags = ListHelper.merge_list(
                    segment_cohort.tags, cohort.tags)
                cohort_api.update(segment_cohort)

    def list_cohorts(self, import_id: str):
        logging.info(
            f"AudienceAPI::list_cohorts::{import_id:}")
        import_segments = self.list_segments(import_id=import_id)
        import_segments = []
        cohort_api = CohortAPI(self.__api_key)
        for import_segment in import_segments:
            cohort = cohort_api.get_by_name(import_segment.name)
            if cohort is not None:
                import_segments.append(cohort)
        return import_segments

    def sync_cohorts(self):
        logging.info(
            f"AudienceAPI:sync_cohorts")
        providers = self.list_imports()
        for provider in providers:
            logging.info(
                f"AudienceAPI:sync_cohorts::{provider.name:}")
            self.create_cohorts(provider.id)
