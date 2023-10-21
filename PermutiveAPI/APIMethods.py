import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Union, Any


from .APIRequestHandler import APIRequestHandler
from .APIDataclasses import Cohort,Import,Segment
from .Utils import FileHelper

COHORT_API_VERSION = 'v2'
COHORT_API_ENDPOINT = f'https://api.permutive.app/cohorts-api/{COHORT_API_VERSION}/cohorts/'


class CohortAPI:
    """
    CohortAPI class
    This class is responsible for interacting with the cohort end points of the permutive API.
    """

    def __init__(self, api_key: str):
        logging.info(f"CohortAPI::__init__")
        self.__api_key = api_key

    def list(self, 
             include_child_workspaces=False) -> List[Cohort]:
        """
            Fetches all cohorts from the API.

            :return: List of all cohorts.
        """
        logging.info(f"CohortAPI::list")
        url = f"{COHORT_API_ENDPOINT}?k={self.__api_key}"
        if include_child_workspaces:
            url = f"{url}&include-child-workspaces=true"
        response = APIRequestHandler.get(url=url)
        return [Cohort(**cohort) for cohort in response.json()]

    def get(self, 
            cohort_id: str) -> Cohort:
        """
        Fetches a specific cohort from the API using its ID.

        :param cohort_id: ID of the cohort.
        :return: Cohort object or None if not found.
        """
        logging.info(f"CohortAPI::get::{cohort_id}")
        url = f"{COHORT_API_ENDPOINT}{cohort_id}?k={self.__api_key}"
        response = APIRequestHandler.get(url=url)

        return Cohort(**response.json())

    def get_by_name(self, 
                    cohort_name: str) -> Optional[Cohort]:
        '''
            Object Oriented Permutive Cohort seqrch
            :rtype: Cohort object
            :param cohort_name: str Cohort Name. Required
            :return: Cohort object
        '''
        logging.info(f"CohortAPI::get_by_name::{cohort_name}")
        for cohort in self.list(include_child_workspaces=True):
            if cohort_name == cohort.name and cohort.id is not None:
                return self.get(cohort.id)

    def get_by_code(self, 
                    cohort_code: Union[int, str]) -> Optional[Cohort]:
        '''
        Object Oriented Permutive Cohort seqrch
        :rtype: Cohort object
        :param cohort_code: Union[int, str] Cohort Code. Required
        :return: Cohort object
        '''
        if type(cohort_code) == str:
            cohort_code = int(cohort_code)
        logging.info(f"CohortAPI::get_by_code::{cohort_code}")
        for cohort in self.list(include_child_workspaces=True):
            if cohort_code == cohort.code and cohort.id is not None:
                return self.get(cohort.id)

    def create(self, 
               cohort: Cohort) -> Cohort:
        """
        Creates a new cohort.

        :param cohort: Cohort to be created.
        :return: Created cohort object.
        """
        if cohort.name is None:
            raise ValueError('name must be specified')
        if cohort.query is None:
            raise ValueError('query must be specified')
        logging.info(f"CohortAPI::create::{cohort.name}")

        url = f"{COHORT_API_ENDPOINT}?k={self.__api_key}"
        response = APIRequestHandler.post(
            url=url,
            data=cohort.to_payload())

        return Cohort(**response.json())

    def update(self, 
               cohort: Cohort) -> Cohort:
        """
        Updates an existing cohort.

        :param cohort_id: ID of the cohort to be updated.
        :param updated_cohort: Updated cohort data.
        :return: Updated cohort object.
        """
        logging.info(f"CohortAPI::update::{cohort.name}")
        if cohort.id is None:
            raise ValueError('id must be specified')
        url = f"{COHORT_API_ENDPOINT}{cohort.id}?k={self.__api_key}"
        APIRequestHandler.patch(
            url=url,
            data=cohort.to_payload())
        return self.get(cohort_id=cohort.id)

    def delete(self, 
               cohort_id: str) -> None:
        """
        Deletes a specific cohort.
        :param cohort_id: ID of the cohort to be deleted.
        :return: None
        """
        logging.info(f"CohortAPI::delete::{cohort_id}")
        url = f"{COHORT_API_ENDPOINT}{cohort_id}?k={self.__api_key}"
        APIRequestHandler.delete(url=url)

    def copy(self, 
             cohort_id: str, k2: Optional[str] = None) -> Cohort:
        """
        Meant for copying a cohort
        :param cohort_id: str the cohort's id to duplicat. Required
        :param k2: str the key to use for creating the copy. Optional. If not specified, uses the current workspace API key
        :return: Response
        :rtype: Response
        """
        logging.info(f"CohortAPI::copy")
        new_cohort = self.get(cohort_id)
        if not new_cohort:
            raise ValueError(f"cohort::{cohort_id} does not exist")

        new_cohort.id = None
        new_cohort.code = None
        new_cohort.name = new_cohort.name + ' (copy)'
        new_description = "Copy of " + cohort_id
        if new_cohort.description is not None:
            new_cohort.description = new_cohort.description + new_description
        else:
            new_cohort.description = new_description
        if k2:
            api = CohortAPI(api_key=k2)
            return api.create(new_cohort)
        return self.create(new_cohort)
    

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

    def list_imports(self) -> List[Import]:
        """
        Fetches all imports from the API.

        :return: List of all imports.
        """
        logging.info(f"AudienceAPI::list_imports")
        url = f"{AUDIENCE_API_ENDPOINT}?k={self.__api_key}"
        response = APIRequestHandler.get(url=url)
        provider_data = response.json()
        return [Import(**provider) for provider in provider_data['items']]

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
        return Import(**response.json())


    def create_segment(self, segment: Segment) -> 'Segment':
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
        return Segment(**response.json())


    def update_segment(self, segment: Segment) -> Segment:
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
        return Segment(**response.json())


    def delete_segment(self, segment: Segment) -> bool:
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
                            segment_id: str) -> Segment:
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
        return Segment(**response.json())


    def get_by_code(self,
                            import_id: str,
                            segment_code: str) -> Segment:
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
        return Segment(**response.json())

    def list_cohorts(self) -> List[Cohort]:
        logging.info(f"AudienceAPI::list_cohorts")
        return CohortAPI(self.__api_key).list(include_child_workspaces=True)
  
    def sync_cohorts(self,
                    import_id: str,
                    masterKey: Optional[str] = None,
                    cohorts_list: Optional[List[Cohort]] = None,
                    prefix: Optional[str] = None,
                    inheritance: bool = False):
        """
            Creates a cohort for each import's segment. 
            Cohort name=f"{prefix or ''}{import.name} | {segment.name}"
            Plus a cohort wrapping up all the import's segment
            Cohort name=f"{prefix or ''}{import.name}"

            :param import_id: str ID of the import.
            :param masterKey: Optional[str] if specified all cohorts will be created on the masterKey Level
            :param cohorts_list: Optional[List[Cohort]] if specified, the reference cohort list; if not API call for the lis
            :param prefix: Optional[str] cohort name are prefixed if specified
            :param inheritance: Optional[str] inherited segment's cohort are created if specified
        """
        logging.info(
            f"AudienceAPI::sync_cohorts::{import_id}")
        import_detail = self.get_import(import_id=import_id)
        if (inheritance and import_detail.inheritance) or \
                (not inheritance and not import_detail.inheritance):
            import_segments = self.list_segments(import_id=import_id)

            if len(import_segments) == 0:
                return
            api_key = masterKey if masterKey is not None else self.__api_key
            if not cohorts_list:
                cohorts_list = CohortAPI(api_key).list(include_child_workspaces=True)
            cohorts_map = {cohort.name: cohort for cohort in cohorts_list}

            name = f"{prefix or ''}{import_detail.name}"
            tags = [import_detail.name, '#automatic', '#imports']
            q_provider_segments = Query(name=name,
                                        id=cohorts_map.get(
                                            name, Cohort(name="")).id,
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
                                    q_segment_name, Cohort(name="")).id,
                                description=f'{t_segment})',
                                tags=tags,
                                second_party_segments=[t_segment])

                q_segment.sync(api_key=api_key)

                q_provider_segments.second_party_segments.append(t_segment)
            if len(q_provider_segments.second_party_segments) > 0:
                logging.info(
                    f"AudienceAPI::sync_cohort::{import_detail.name}")
                q_provider_segments.sync(api_key=api_key)

    def list_segments(self,
                    import_id: str,
                    pagination_token: Optional[str] = None) -> List[Segment]:
        """
        Fetches all segments for a specific import.

        :param import_id: ID of the import.
        :return: List of all segments.
        """
        logging.info(f"AudienceAPI::list_segments::{import_id}")
        url = f"{AUDIENCE_API_ENDPOINT}/{import_id}/segments?k={self.__api_key}"
        if pagination_token:
            url = f"{url}&pagination_token={pagination_token}"
        response = APIRequestHandler.get(url=url)
        if response is None:
            raise ValueError('Unable to list_segments')
        response_json = response.json()
        segments = [Segment(
            **segment) for segment in response_json['elements']]
        next_token = response_json.get(
            'pagination', {}).get('next_token', None)
        if next_token:
            logging.info(
                f"AudienceAPI::list_segments::{import_id}::{next_token}")
            segments += self.list_segments(import_id=import_id,pagination_token=next_token)
        return segments

   

   
    def sync_imports_cohorts(self,
                                masterKey: Optional[str] = None,
                                prefix: Optional[str] = None,
                                inheritance: bool = False):
        logging.info(f"AudienceAPI:sync_imports_cohorts")
        imports = self.list_imports()

        for item in imports:
            self.sync_cohorts(import_id=item.id,
                                            masterKey=masterKey,
                                            prefix=prefix,
                                            inheritance=inheritance)
