from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional
import logging
from .APIRequestHandler import APIRequestHandler
from .Query import Query
from .CohortAPI import CohortAPI
from .Utils import FileHelper


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
        Dataclass for the Provider Import in the Permutive ecosystem.
        """
        id: str
        name: str
        code: str
        relation: str
        identifiers: List[str]
        description: Optional[str] = None
        source: Optional['Source'] = None
        inheritance: Optional[str] = None
        segments: Optional[List['Segment']] = None
        updated_at: Optional[datetime] = datetime.now()

        def __init__(self, id, name, code, relation, identifiers, description=None,
                     source=None, inheritance=None, api_key: Optional[str] = None):
            self.id = id
            self.name = name
            self.code = code
            self.relation = relation
            self.identifiers = identifiers
            self.description = description
            self.source = source
            self.inheritance = inheritance
            if api_key:
                self.segments = AudienceAPI.Import.list_segments(api_key=api_key,import_id=self.id)
            self.updated_at = datetime.now()

        def to_json(self, filepath: str):
            FileHelper.to_json(self, filepath=filepath)

        @staticmethod
        def from_json(filepath: str) -> 'AudienceAPI.Import':
            jsonObj = FileHelper.from_json(filepath=filepath)
            return AudienceAPI.Import(**jsonObj)

        @staticmethod
        def get_import(api_key: str,
                       import_id: str) -> 'AudienceAPI.Import':
            """
            Fetches a specific import by its id.

            :param import_id: ID of the import.
            :return: The requested Importt.
            """
            logging.info(f"AudienceAPI::get_import::{import_id}")
            url = f"{AUDIENCE_API_ENDPOINT}/{import_id}?k={api_key}"
            response = APIRequestHandler.get(url=url)
            if response is None:
                raise ValueError('Unable to get_import')
            return AudienceAPI.Import(**response.json())
        @staticmethod
        def sync_cohorts(api_key: str,
                     import_id: str,
                     masterKey: Optional[str] = None,
                     cohorts_list: Optional[List[CohortAPI.Cohort]] = None,
                     prefix: Optional[str] = None,
                     inheritance: bool = False):
            """
                Creates a cohort for each import's segment. 
                Cohort name=f"{prefix or ''}{import.name} | {segment.name}"
                Plus a cohort wrapping up all the import's segment
                Cohort name=f"{prefix or ''}{import.name}"

                :param import_id: str ID of the import.
                :param masterKey: Optional[str] if specified all cohorts will be created on the masterKey Level
                :param cohorts_list: Optional[List[CohortAPI.Cohort]] if specified, the reference cohort list; if not API call for the lis
                :param prefix: Optional[str] cohort name are prefixed if specified
                :param inheritance: Optional[str] inherited segment's cohort are created if specified
            """
            logging.info(
                f"AudienceAPI::sync_cohorts::{import_id}")
            import_detail = AudienceAPI.Import.get_import(api_key=api_key,
                                                          import_id=import_id)
            if (inheritance and import_detail.inheritance) or \
                    (not inheritance and not import_detail.inheritance):
                import_segments = AudienceAPI.Import.list_segments(api_key=api_key,
                                                                   import_id=import_id)

                if len(import_segments) == 0:
                    return
                api_key = masterKey if masterKey is not None else api_key
                if not cohorts_list:
                    cohorts_list = CohortAPI(api_key).list(include_child_workspaces=True)
                cohorts_map = {cohort.name: cohort for cohort in cohorts_list}

                name = f"{prefix or ''}{import_detail.name}"
                tags = [import_detail.name, '#automatic', '#imports']
                q_provider_segments = Query(name=name,
                                            id=cohorts_map.get(
                                                name, CohortAPI.Cohort(name="")).id,
                                            description=f"Wrap-up of all {import_detail.name}'s segments",
                                            tags=tags)
                q_provider_segments.second_party_segments = []
                for import_segment in import_segments:

                    logging.info(
                        f"AudienceAPI::sync_cohort::{import_detail.name}::{import_segment.name}")
                    t_segment = (import_detail.code, import_segment.code)
                    q_segment_name = f"{name} | {import_segment.name}"
                    q_segment = Query(name=q_segment_name,
                                    id=cohorts_map.get(
                                        q_segment_name, CohortAPI.Cohort(name="")).id,
                                    description=f'{t_segment})',
                                    tags=tags,
                                    second_party_segments=[t_segment])

                    q_segment.sync(api_key=api_key)

                    q_provider_segments.second_party_segments.append(t_segment)
                if len(q_provider_segments.second_party_segments) > 0:
                    logging.info(
                        f"AudienceAPI::sync_cohort::{import_detail.name}")
                    q_provider_segments.sync(api_key=api_key)
        @staticmethod
        def list_segments(api_key: str,
                      import_id: str,
                      pagination_token: Optional[str] = None) -> List['AudienceAPI.Import.Segment']:
            """
            Fetches all segments for a specific import.

            :param import_id: ID of the import.
            :return: List of all segments.
            """
            logging.info(f"AudienceAPI::list_segments::{import_id}")
            url = f"{AUDIENCE_API_ENDPOINT}/{import_id}/segments?k={api_key}"
            if pagination_token:
                url = f"{url}&pagination_token={pagination_token}"
            response = APIRequestHandler.get(url=url)
            if response is None:
                raise ValueError('Unable to list_segments')
            response_json = response.json()
            segments = [AudienceAPI.Import.Segment(
                **segment) for segment in response_json['elements']]
            next_token = response_json.get(
                'pagination', {}).get('next_token', None)
            if next_token:
                logging.info(
                    f"AudienceAPI::list_segments::{import_id}::{next_token}")
                segments += AudienceAPI.Import.list_segments(api_key=api_key,
                                                             import_id=import_id,
                                                             pagination_token=next_token)
            return segments

   
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
            import_id: str
            description: Optional[str] = None
            cpm: Optional[float] = 0.0
            categories: Optional[List[str]] = None
            updated_at: Optional[datetime] = datetime.now()

            def to_json(self, filepath: str):
                FileHelper.to_json(self, filepath=filepath)

            def create(self, api_key: str):
                api=AudienceAPI(api_key=api_key)
                api.create_segment(segment=self)

            def update(self, api_key: str):
                api=AudienceAPI(api_key=api_key)
                api.update_segment(segment=self)

            def delete(self, api_key: str):
                api=AudienceAPI(api_key=api_key)
                api.delete_segment(segment=self)

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
        Fetches all imports from the API.

        :return: List of all imports.
        """
        logging.info(f"AudienceAPI::list_imports")
        url = f"{AUDIENCE_API_ENDPOINT}?k={self.__api_key}"
        response = APIRequestHandler.get(url=url)
        provider_data = response.json()
        return [AudienceAPI.Import(**provider) for provider in provider_data['items']]

    def get_import(self,
                   import_id: str) -> Import:
        """
        Fetches a specific import by its id.

        :param import_id: ID of the import.
        :return: The requested Importt.
        """
        logging.info(f"AudienceAPI::get_import::{import_id}")
        url = f"{AUDIENCE_API_ENDPOINT}/{import_id}?k={self.__api_key}"
        response = APIRequestHandler.get(url=url)
        if response is None:
            raise ValueError('Unable to get_import')
        return AudienceAPI.Import(**response.json())


    def create_segment(self, segment: 'AudienceAPI.Import.Segment') -> 'AudienceAPI.Import.Segment':
        """
        Creates a new segment

        :return: The created Segment.
        """
        logging.info(
            f"AudienceAPI::create_segment::{segment.import_id}::{segment.name}")
        url = f"{AUDIENCE_API_ENDPOINT}/{segment.import_id}/segments?k={self.__api_key}"
        payload = ['name', 'code', 'description', 'cpm', 'categories']
        response = APIRequestHandler.post(
            url=url, data=APIRequestHandler.to_payload(payload))
        if response is None:
            raise ValueError('Unable to create_segment')
        return AudienceAPI.Import.Segment(**response.json())


    def update_segment(self, segment: 'AudienceAPI.Import.Segment') -> 'AudienceAPI.Import.Segment':
        """
        PATCH
        https://api.permutive.app/audience-api/v1/imports/{importId}/segments/{segmentId}
        Updates a segment for an import. The segment is identified by its globally unique public ID.
        https://developer.permutive.com/reference/patchimportsimportidsegmentssegmentid
        :param segment: AudienceAPI.Import.Segment to update
        :return: The updated Segment.
        """

        logging.info(
            f"AudienceAPI::update_segment::{segment.import_id}::{segment.name}")
        url = f"{AUDIENCE_API_ENDPOINT}/{segment.import_id}/segments/{segment.id}?k={self.__api_key}"
        payload = ['name', 'code', 'description', 'cpm', 'categories']
        response = APIRequestHandler.patch(
            url=url,  data=APIRequestHandler.to_payload(segment, payload))
        if response is None:
            raise ValueError('Unable to update_segment')
        return AudienceAPI.Import.Segment(**response.json())


    def delete_segment(self, segment: 'AudienceAPI.Import.Segment') -> bool:
        """
        Deletes a specific segment by its id.

        :param import_id: ID of the import.
        :param segment_id: ID of the segment.
        :return: True if deletion was successful, otherwise False.
        """
        logging.info(
            f"AudienceAPI::delete_segment::{segment.import_id:}::{segment.id}")
        url = f"{AUDIENCE_API_ENDPOINT}/{segment.import_id}/segments/{segment.id}?k={self.__api_key}"
        response = APIRequestHandler.delete(url=url)
        return response.status_code == 204


    def get_segment(self,
                            import_id: str,
                            segment_id: str) -> 'AudienceAPI.Import.Segment':
        """
        Fetches a specific segment by its id.
        https://developer.permutive.com/reference/getimportsimportidsegmentssegmentid
        :param import_id: ID of the import.
        :param segment_id: UUID of the segment.
        :return: The requested Segment.
        """
        logging.info(
            f"AudienceAPI::get_segment_by_id::{import_id}::{segment_id}")
        url = f"{AUDIENCE_API_ENDPOINT}/{import_id}/segments/{segment_id}?k={self.__api_key}"
        response = APIRequestHandler.get(url=url)
        if response is None:
            raise ValueError('Unable to get_segment')
        return AudienceAPI.Import.Segment(**response.json())


    def get_by_code(self,
                            import_id: str,
                            segment_code: str) -> 'AudienceAPI.Import.Segment':
        """
        Fetches a specific segment by its code.
        https://developer.permutive.com/reference/getimportsimportidsegmentscodesegmentcode
        :param import_id: ID of the import.
        :param segment_code: Public code of the segment.
        :return: The requested Segment.
        """
        logging.info(
            f"AudienceAPI::get_segment_by_code::{import_id}::{segment_code}")
        url = f"{AUDIENCE_API_ENDPOINT}/{import_id}/segments/code/{segment_code}?k={self.__api_key}"
        response = APIRequestHandler.get(url=url)
        if response is None:
            raise ValueError('Unable to get_segment')
        return AudienceAPI.Import.Segment(**response.json())

    def list_cohorts(self) -> List[CohortAPI.Cohort]:
        logging.info(f"AudienceAPI::list_cohorts")
        return CohortAPI(self.__api_key).list(include_child_workspaces=True)

    def sync_imports_cohorts(self,
                             masterKey: Optional[str] = None,
                             prefix: Optional[str] = None,
                             inheritance: bool = False):
        logging.info(f"AudienceAPI:sync_imports_cohorts")
        imports = self.list_imports()

        for item in imports:
            AudienceAPI.Import.sync_cohorts(api_key=self.__api_key,
                                            import_id=item.id,
                                            masterKey=masterKey,
                                            prefix=prefix,
                                            inheritance=inheritance)
