from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional
import logging
from .APIRequestHandler import APIRequestHandler
from .Query import Query
from .CohortAPI import CohortAPI
from .Utils import ListHelper, FileHelper

AUDIENCE_API_VERSION = 'v1'
AUDIENCE_API_ENDPOINT = f'https://api.permutive.app/audience-api/{AUDIENCE_API_VERSION}/imports'


class AudienceAPI:

    """
        AudienceAPI class
        This class is responsible for interacting with the audience end points of the permutive API.
    """
    # region dataclasses
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
        description: Optional[Dict] = None
        source: Optional['Source'] = None
        inheritance: Optional[str] = None
        segments: Optional[List['Segment']] = None
        updated_at: Optional[datetime] = datetime.now()

        def to_json(self, filepath: str):
            FileHelper.to_json(self, filepath=filepath)

        @staticmethod
        def from_json(filepath: str) -> 'AudienceAPI.Import':
            jsonObj = FileHelper.from_json(filepath=filepath)
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

            def to_json(self, filepath: str):
                FileHelper.to_json(self, filepath=filepath)

            @staticmethod
            def from_json(filepath: str) -> 'AudienceAPI.Import.Source':
                jsonObj = FileHelper.from_json(filepath=filepath)
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

            def to_json(self, filepath: str):
                FileHelper.to_json(self, filepath=filepath)

            @staticmethod
            def from_json(filepath: str) -> 'AudienceAPI.Import.Segment':
                jsonObj = FileHelper.from_json(filepath=filepath)
                return AudienceAPI.Import.Segment(**jsonObj)
    # endregion

    def __init__(self, api_key: str):
        logging.info(f"AudienceAPI::__init__")
        self.__api_key = api_key

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

    def get_import(self,  import_id: str) -> Import:
        """
        Fetches a specific import by its id.

        :param data_provider_id: ID of the data provider.
        :return: The requested Importt.
        """
        logging.info(f"AudienceAPI::get_import::{import_id}")
        url = f"{AUDIENCE_API_ENDPOINT}/{import_id}?k={self.__api_key}"
        response = APIRequestHandler.get(url=url)
        if response is None:
            raise ValueError('Unable to get_import')
        return AudienceAPI.Import(**response.json())

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
        if response is None:
            raise ValueError('Unable to get_segment')
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
        url = f"{AUDIENCE_API_ENDPOINT}/{segment.import_id}/segments?k={self.__api_key}"
        payload = ['name', 'code', 'description', 'cpm', 'categories']
        response = APIRequestHandler.post(
            url=url, data=APIRequestHandler.to_payload(payload))
        return AudienceAPI.Import.Segment(**response.json())

    def update_segment(self, segment: Import.Segment) -> Import.Segment:
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

    def sync_cohorts(self, import_id: str,
                     masterKey: Optional[str] = None,
                     cohorts_list: Optional[List[CohortAPI.Cohort]] = None,
                     prefix: Optional[str] = None,

                     inheritance: bool = False):
        logging.info(
            f"AudienceAPI::sync_cohorts::{import_id:}")
        import_detail = self.get_import(import_id=import_id)
        if (inheritance and import_detail.inheritance) or \
                (not inheritance and not import_detail.inheritance):
            import_segments = self.list_segments(import_id=import_id)

            if len(import_segments) == 0:
                return
            api_key = masterKey if masterKey is not None else self.__api_key
            if not cohorts_list:
                cohorts_list = self.list_cohorts()

            q_provider_segments = Query(name=f"{prefix or ''}{import_detail.name}",
                                        tags=[import_detail.name,
                                              '#automatic', '#imports'],
                                        second_party_segments=[])
            q_provider_segments.id = next(
                (cohort.id for cohort in cohorts_list if cohort.name == q_provider_segments.name), None)

            if q_provider_segments.id:
                cohort_tags = next(
                    (cohort.tags for cohort in cohorts_list if cohort.id == q_provider_segments.id), None)
                q_provider_segments.tags = ListHelper.merge_list(
                    q_provider_segments.tags, cohort_tags)
            for import_segment in import_segments:

                logging.info(
                    f"AudienceAPI::sync_cohort::{import_detail.name}::{import_segment.name}")
                t_segment = (import_detail.code, import_segment.code)

                q_segment = Query(name=f"{prefix or ''}{import_detail.name} | {import_segment.name}",
                                  description=f'{import_detail.name} ({import_detail.id}) : {import_segment.code} : {import_segment.name} ({import_segment.id})',
                                  tags=[import_detail.name,
                                        '#automatic', '#imports'],
                                  second_party_segments=[t_segment])
                q_segment.id = next(
                    (cohort.id for cohort in cohorts_list if cohort.name == q_segment.name), None)

                if q_segment.id:
                    cohort_tags = next(
                        (cohort.tags for cohort in cohorts_list if cohort.id == q_segment.id), None)
                    q_segment.tags = ListHelper.merge_list(
                        q_segment.tags, cohort_tags)
                q_segment.sync(api_key=api_key)
                q_provider_segments.second_party_segments.append(t_segment)
            logging.info(
                f"AudienceAPI::sync_cohort::{import_detail.name}")
            q_provider_segments.sync(api_key=api_key)

    def list_cohorts(self) -> List[CohortAPI.Cohort]:
        logging.info(f"AudienceAPI::list_cohorts")
        return CohortAPI(self.__api_key).list(include_child_workspaces=True)

    def sync_imports_cohorts(self,
                             masterKey: Optional[str] = None,
                             prefix: Optional[str] = None,

                             inheritance: bool = False):
        logging.info(f"AudienceAPI:sync_imports_cohorts")
        providers = self.list_imports()

        for provider in providers:
            self.sync_cohorts(provider.id,
                              masterKey,
                              prefix=prefix,
                              inheritance=inheritance)
