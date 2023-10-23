
import logging
from typing import Dict, List, Optional, Any,Union
from dataclasses import dataclass
from datetime import datetime
import requests
from requests.exceptions import RequestException
from requests.models import Response
import json
from .Utils import FileHelper


class APIRequestHandler:
    """
        A utility class for making HTTP requests to a RESTful API and handling common operations.

        Attributes:
            DEFAULT_HEADERS (dict): Default HTTP headers used for API requests.
    """
    DEFAULT_HEADERS = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

    @staticmethod
    def handle_exception(response: Optional[Response], e: Exception):
        """
            Handle exceptions and errors in API requests.

            This method checks the HTTP response and handles exceptions gracefully. It logs error messages and raises exceptions when necessary.

            Args:
                response (Optional[Response]): The HTTP response object.
                e (Exception): The exception that occurred during the request.

            Returns:
                Response: The HTTP response object if it's successful or a 400 Bad Request response. Otherwise, it raises the original exception.

        """
        if response is not None:
            if 200 <= response.status_code <= 300:
                return response
            elif response.status_code == 400:
                try:
                    error_content = json.loads(response.content)
                    error_message = error_content.get(
                        "error", {}).get("cause", "Unknown error")
                except json.JSONDecodeError:
                    error_message = "Could not parse error message"

                logging.warning(f"Received a 400 Bad Request: {error_message}")
                return response
        logging.error(f"An error occurred: {e}")
        raise e

    @staticmethod
    def get(url: str, headers: Optional[Dict[str, str]] = None) -> Response:
        """
            Send an HTTP GET request to the specified URL.

            Args:
                url (str): The URL to send the GET request to.
                headers (Optional[Dict[str, str]]): Custom headers to include in the request. Defaults to None.

            Returns:
                Response: The HTTP response object.

        """
        headers = headers or APIRequestHandler.DEFAULT_HEADERS
        response = None
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
        except RequestException as e:
            return APIRequestHandler.handle_exception(response, e)
        return response

    @staticmethod
    def post(url: str, data: dict, headers: Optional[Dict[str, str]] = None) -> Response:
        """
            Send an HTTP POST request to the specified URL with JSON data.

            Args:
                url (str): The URL to send the POST request to.
                data (dict): The JSON data to include in the request body.
                headers (Optional[Dict[str, str]]): Custom headers to include in the request. Defaults to None.

            Returns:
                Response: The HTTP response object.

        """
        headers = headers or APIRequestHandler.DEFAULT_HEADERS
        response = None
        try:
            response = requests.post(url, headers=headers, json=data)
            response.raise_for_status()
        except RequestException as e:
            return APIRequestHandler.handle_exception(response, e)
        return response

    @staticmethod
    def patch(url: str, data: dict, headers: Optional[Dict[str, str]] = None) -> Response:
        """
            Send an HTTP PATCH request to the specified URL with JSON data.

            Args:
                url (str): The URL to send the PATCH request to.
                data (dict): The JSON data to include in the request body.
                headers (Optional[Dict[str, str]]): Custom headers to include in the request. Defaults to None.

            Returns:
                Response: The HTTP response object.

        """        
        headers = headers or APIRequestHandler.DEFAULT_HEADERS
        response = None
        try:
            response = requests.patch(url, headers=headers, json=data)
            response.raise_for_status()
        except RequestException as e:
            return APIRequestHandler.handle_exception(response, e)
        return response

    @staticmethod
    def delete(url: str, headers: Optional[Dict[str, str]] = None) -> Response:
        """
            Send an HTTP DELETE request to the specified URL.

            Args:
                url (str): The URL to send the DELETE request to.
                headers (Optional[Dict[str, str]]): Custom headers to include in the request. Defaults to None.

            Returns:
                Response: The HTTP response object.

        """        
        headers = headers or APIRequestHandler.DEFAULT_HEADERS
        response = None
        try:
            response = requests.delete(url, headers=headers)
            response.raise_for_status()
        except RequestException as e:
            return APIRequestHandler.handle_exception(response, e)
        return response

    @staticmethod
    def to_payload(dataclass_obj: Any, keys: Optional[List[str]] = None) -> Dict[str, Any]:
        """
            Convert a data class object to a dictionary payload.

            This method converts a data class object into a dictionary, optionally filtering keys.

            Args:
                dataclass_obj (Any): The data class object to be converted.
                keys (Optional[List[str]]): List of keys to include in the payload. If None, all keys with non-None values are included.

            Returns:
                Dict[str, Any]: The dictionary payload.

        """
        if keys:
            return {key: value for key, value in vars(dataclass_obj).items() if value is not None and key in keys}
        return {key: value for key, value in vars(dataclass_obj).items() if value is not None}


COHORT_API_VERSION = 'v2'
COHORT_API_ENDPOINT = f'https://api.permutive.app/cohorts-api/{COHORT_API_VERSION}/cohorts/'
COHORT_PAYLOAD_DICT=["name", "query", "description", "tags"]

@dataclass
class Cohort(FileHelper):
    """
    Dataclass for the Cohort entity in the Permutive ecosystem.
    """
    name: str
    id: Optional[str] = None
    code: Optional[str] = None
    query: Optional[Dict] = None
    tags: Optional[List[str]] = None
    description: Optional[str] = None
    state: Optional[str] = None
    segment_type: Optional[str] = None
    live_audience_size: Optional[int] = 0
    created_at: Optional[datetime] = datetime.now()
    last_updated_at: Optional[datetime] = datetime.now()
    workspace_id: Optional[str] = None
    request_id: Optional[str] = None
    error: Optional[str] = None


    @staticmethod
    def list(api_key:str, 
             include_child_workspaces=False) -> List['Cohort']:
        """
            Fetches all cohorts from the API.

            :return: List of all cohorts.
        """
        logging.info(f"CohortAPI::list")
        url = f"{COHORT_API_ENDPOINT}?k={api_key}"
        if include_child_workspaces:
            url = f"{url}&include-child-workspaces=true"
        response = APIRequestHandler.get(url=url)
        return [Cohort(**cohort) for cohort in response.json()]
    @staticmethod
    def get(api_key:str, 
            cohort_id: str) ->'Cohort':
        """
        Fetches a specific cohort from the API using its ID.

        :param cohort_id: ID of the cohort.
        :return: Cohort object or None if not found.
        """
        logging.info(f"CohortAPI::get::{cohort_id}")
        url = f"{COHORT_API_ENDPOINT}{cohort_id}?k={api_key}"
        response = APIRequestHandler.get(url=url)

        return Cohort(**response.json())
    @staticmethod
    def get_by_name(api_key:str, 
                    cohort_name: str
                    ) -> Optional['Cohort']:
        '''
            Object Oriented Permutive Cohort seqrch
            :rtype: Cohort object
            :param cohort_name: str Cohort Name. Required
            :return: Cohort object
        '''
        logging.info(f"CohortAPI::get_by_name::{cohort_name}")
        
        for cohort in Cohort.list(api_key=api_key,
                                  include_child_workspaces=True):
            if cohort_name == cohort.name and cohort.id is not None:
                return Cohort.get(api_key=api_key,
                                  cohort_id=cohort.id)
    @staticmethod
    def get_by_code(api_key:str, 
                    cohort_code: Union[int, str]) -> Optional['Cohort']:
        '''
        Object Oriented Permutive Cohort seqrch
        :rtype: Cohort object
        :param cohort_code: Union[int, str] Cohort Code. Required
        :return: Cohort object
        '''
        if type(cohort_code) == str:
            cohort_code = int(cohort_code)
        logging.info(f"CohortAPI::get_by_code::{cohort_code}")
        for cohort in Cohort.list(api_key=api_key,
                                  include_child_workspaces=True):
            if cohort_code == cohort.code and cohort.id is not None:
                return Cohort.get(api_key=api_key,
                                  cohort_id=cohort.id)     
    @staticmethod
    def create(api_key:str,  
               cohort: 'Cohort') -> 'Cohort':
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

        url = f"{COHORT_API_ENDPOINT}?k={api_key}"
        response = APIRequestHandler.post(
            url=url,
            data=APIRequestHandler.to_payload(cohort,COHORT_PAYLOAD_DICT))

        return Cohort(**response.json())
    @staticmethod
    def update(api_key:str, 
               cohort: 'Cohort') -> 'Cohort':
        """
        Updates an existing cohort.

        :param cohort_id: ID of the cohort to be updated.
        :param updated_cohort: Updated cohort data.
        :return: Updated cohort object.
        """
        logging.info(f"CohortAPI::update::{cohort.name}")
        if cohort.id is None:
            raise ValueError('id must be specified')
        url = f"{COHORT_API_ENDPOINT}{cohort.id}?k={api_key}"
        APIRequestHandler.patch(
            url=url,
            data=APIRequestHandler.to_payload(cohort,COHORT_PAYLOAD_DICT))
        return Cohort.get(api_key=api_key,
                          cohort_id=cohort.id)
    @staticmethod
    def delete(api_key:str,  
               cohort: 'Cohort') -> None:
        """
        Deletes a specific cohort.
        :param cohort_id: ID of the cohort to be deleted.
        :return: None
        """
        logging.info(f"CohortAPI::delete::{cohort.id}")
        url = f"{COHORT_API_ENDPOINT}{cohort.id}?k={api_key}"
        APIRequestHandler.delete(url=url)
    @staticmethod
    def copy(api_key:str, 
             cohort_id: str, k2: Optional[str] = None) -> 'Cohort':
        """
        Meant for copying a cohort
        :param cohort_id: str the cohort's id to duplicat. Required
        :param k2: str the key to use for creating the copy. Optional. If not specified, uses the current workspace API key
        :return: Response
        :rtype: Response
        """
        logging.info(f"CohortAPI::copy")
        new_cohort = Cohort.get(api_key=api_key,
                                cohort_id=cohort_id)
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
            return Cohort.create(api_key=k2,cohort=new_cohort)
        return Cohort.create(api_key=api_key,cohort=new_cohort)




AUDIENCE_API_VERSION = 'v1'
AUDIENCE_API_ENDPOINT = f'https://api.permutive.app/audience-api/{AUDIENCE_API_VERSION}/imports'


@dataclass
class Import(FileHelper):
    """
    Dataclass for the Import in the Permutive ecosystem.
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

    @staticmethod
    def list(api_key:str) -> List['Import']:
        """
        Fetches all imports from the API.

        :return: List of all imports.
        """
        logging.info(f"AudienceAPI::list_imports")
        url = f"{AUDIENCE_API_ENDPOINT}?k={api_key}"
        response = APIRequestHandler.get(url=url)
        provider_data = response.json()
        return [Import(**provider) for provider in provider_data['items']]
    @staticmethod
    def get(api_key:str,
                   import_id: str) -> 'Import':
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
        return Import(**response.json())
    @staticmethod
    def list_segments(api_key:str,
                      import_id:str,
                      pagination_token:Optional[str]=None)->List['Segment']:
       return Segment.list(api_key=api_key,
                     import_id=import_id,
                     pagination_token=pagination_token)

@dataclass
class Segment(FileHelper):
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

    @staticmethod
    def create(api_key:str, segment: 'Segment') -> 'Segment':
        """
        Creates a new segment

        :return: The created Segment.
        """
        logging.info(
            f"AudienceAPI::create_segment::{segment.import_id}::{segment.name}")
        url = f"{AUDIENCE_API_ENDPOINT}/{segment.import_id}/segments?k={api_key}"
        payload = ['name', 'code', 'description', 'cpm', 'categories']
        response = APIRequestHandler.post(
            url=url, data=APIRequestHandler.to_payload(payload))
        if response is None:
            raise ValueError('Unable to create_segment')
        return Segment(**response.json())

    @staticmethod
    def update(api_key:str, segment: 'Segment') -> 'Segment':
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
        url = f"{AUDIENCE_API_ENDPOINT}/{segment.import_id}/segments/{segment.id}?k={api_key}"
        payload = ['name', 'code', 'description', 'cpm', 'categories']
        response = APIRequestHandler.patch(
            url=url,  data=APIRequestHandler.to_payload(segment, payload))
        if response is None:
            raise ValueError('Unable to update_segment')
        return Segment(**response.json())

    @staticmethod
    def delete(api_key:str, segment: 'Segment') -> bool:
        """
        Deletes a specific segment by its id.

        :param import_id: ID of the import.
        :param segment_id: ID of the segment.
        :return: True if deletion was successful, otherwise False.
        """
        logging.info(
            f"AudienceAPI::delete_segment::{segment.import_id:}::{segment.id}")
        url = f"{AUDIENCE_API_ENDPOINT}/{segment.import_id}/segments/{segment.id}?k={api_key}"
        response = APIRequestHandler.delete(url=url)
        return response.status_code == 204

    @staticmethod
    def get(api_key:str,
                            import_id: str,
                            segment_id: str) -> 'Segment':
        """
        Fetches a specific segment by its id.
        https://developer.permutive.com/reference/getimportsimportidsegmentssegmentid
        :param import_id: ID of the import.
        :param segment_id: UUID of the segment.
        :return: The requested Segment.
        """
        logging.info(
            f"AudienceAPI::get_segment_by_id::{import_id}::{segment_id}")
        url = f"{AUDIENCE_API_ENDPOINT}/{import_id}/segments/{segment_id}?k={api_key}"
        response = APIRequestHandler.get(url=url)
        if response is None:
            raise ValueError('Unable to get_segment')
        return Segment(**response.json())

    @staticmethod
    def get_by_code(api_key:str,
                            import_id: str,
                            segment_code: str) -> 'Segment':
        """
        Fetches a specific segment by its code.
        https://developer.permutive.com/reference/getimportsimportidsegmentscodesegmentcode
        :param import_id: ID of the import.
        :param segment_code: Public code of the segment.
        :return: The requested Segment.
        """
        logging.info(
            f"AudienceAPI::get_segment_by_code::{import_id}::{segment_code}")
        url = f"{AUDIENCE_API_ENDPOINT}/{import_id}/segments/code/{segment_code}?k={api_key}"
        response = APIRequestHandler.get(url=url)
        if response is None:
            raise ValueError('Unable to get_segment')
        return Segment(**response.json())

    @staticmethod
    def list(api_key:str,
                    import_id: str,
                    pagination_token: Optional[str] = None) -> List['Segment']:
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
        segments = [Segment(
            **segment) for segment in response_json['elements']]
        next_token = response_json.get(
            'pagination', {}).get('next_token', None)
        if next_token:
            logging.info(
                f"AudienceAPI::list_segments::{import_id}::{next_token}")
            segments +=  Segment.list(api_key=api_key,
                                               import_id=import_id,
                                               pagination_token=next_token)
        return segments


USER_API_VERSION = 'v2.0'
USER_API_ENDPOINT =  f'https://api.permutive.com/{USER_API_VERSION}/identify'


class UserAPI():

    def __init__(self, api_key: str):
        logging.info(f"UserAPI::__init__")
        self.__api_key = api_key

    def identify(self,
                 user_id: str,
                 id: str,
                 tag: str = "email_sha256",
                 priority: int = 0):

        if user_id is None:
            raise ValueError('user_id must be specified')
        if id is None:
            raise ValueError('id must be specified')
        logging.info(f"UserAPI::identify::{user_id}")

        url = f"{USER_API_VERSION}?k={self.__api_key}"
        aliases = [
            {
                "tag": tag,
                "id": id,
                "priority": priority
            }
        ]
        if tag == "email_sha256":
            aliases.append({
                "tag": "uID",
                "id": id,
                "priority": priority
            })
        if tag == "uID":
            aliases.append({
                "tag": "email_sha256",
                "id": id,
                "priority": priority
            })
        payload = {
            "aliases": aliases,
            "user_id": user_id
        }
        return APIRequestHandler.post(
            url=url,
            data=payload)
