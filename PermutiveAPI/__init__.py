import logging
import os
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field
import json


import requests
from requests.exceptions import RequestException
from requests.models import Response
import json
import datetime
import pathlib
from glob import glob
import ast


class RequestHelper:
    """
        A utility class for making HTTP requests to a RESTful API and handling common operations.

        Attributes:
            DEFAULT_HEADERS (dict): Default HTTP headers used for API requests.
    """
    DEFAULT_HEADERS = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    api_key: str
    api_endpoint: str
    payload_keys: Optional[List[str]] = None

    def __init__(self,
                 api_key: str,
                 api_endpoint: str,
                 payload_keys: Optional[List[str]] = None) -> None:
        self.api_key = api_key
        self.api_endpoint = api_endpoint
        self.payload_keys = payload_keys

    @staticmethod
    def gen_url_with_key(url, privateKey):
        if "?" in url:
            return f"{url}&k={privateKey}"
        else:
            return f"{url}?k={privateKey}"

    @staticmethod
    def getRequest_static(privateKey: str, url: str) -> Response:
        response = None
        url = RequestHelper.gen_url_with_key(url, privateKey)
        try:
            response = requests.get(
                url, headers=RequestHelper.DEFAULT_HEADERS)
            response.raise_for_status()
        except RequestException as e:
            return RequestHelper.handle_exception(response, e)
        return response

    @staticmethod
    def postRequest_static(privateKey: str,
                           url: str,
                           data: dict) -> Response:
        """
            Send an HTTP POST request to the specified URL with JSON data.

            Args:
                url (str): The URL to send the POST request to.
                data (dict): The JSON data to include in the request body.
                headers (Optional[Dict[str, str]]): Custom headers to include in the request. Defaults to None.

            Returns:
                Response: The HTTP response object.

        """
        response = None
        url = RequestHelper.gen_url_with_key(url, privateKey)
        try:
            response = requests.post(url,
                                     headers=RequestHelper.DEFAULT_HEADERS,
                                     json=data)
            response.raise_for_status()
        except RequestException as e:
            return RequestHelper.handle_exception(response, e)
        return response

    @staticmethod
    def patchRequest_static(privateKey: str,
                            url: str,
                            data: dict) -> Response:
        """
            Send an HTTP PATCH request to the specified URL with JSON data.

            Args:
                url (str): The URL to send the PATCH request to.
                data (dict): The JSON data to include in the request body.
                headers (Optional[Dict[str, str]]): Custom headers to include in the request. Defaults to None.

            Returns:
                Response: The HTTP response object.

        """
        response = None
        url = RequestHelper.gen_url_with_key(url=url,
                                             privateKey=privateKey)
        try:
            response = requests.patch(url,
                                      headers=RequestHelper.DEFAULT_HEADERS,
                                      json=data)
            response.raise_for_status()
        except RequestException as e:
            return RequestHelper.handle_exception(response, e)
        return response

    @staticmethod
    def deleteRequest_static(privateKey: str, url: str) -> Response:
        """
            Send an HTTP DELETE request to the specified URL.

            Args:
                url (str): The URL to send the DELETE request to.
                headers (Optional[Dict[str, str]]): Custom headers to include in the request. Defaults to None.

            Returns:
                Response: The HTTP response object.

        """
        response = None
        url = RequestHelper.gen_url_with_key(
            url=url, privateKey=privateKey)
        try:
            response = requests.delete(url,
                                       headers=RequestHelper.DEFAULT_HEADERS)
            response.raise_for_status()
        except RequestException as e:
            return RequestHelper.handle_exception(response, e)
        return response

    def getRequest(self,
                   url) -> Response:
        response = None
        url = RequestHelper.gen_url_with_key(
            url=url, privateKey=self.api_key)
        try:
            response = requests.get(url, headers=self.DEFAULT_HEADERS)
            response.raise_for_status()
        except RequestException as e:
            return RequestHelper.handle_exception(response, e)
        return response

    def postRequest(self,
                    url: str,
                    data: dict) -> Response:
        """
            Send an HTTP POST request to the specified URL with JSON data.

            Args:
                url (str): The URL to send the POST request to.
                data (dict): The JSON data to include in the request body.
                headers (Optional[Dict[str, str]]): Custom headers to include in the request. Defaults to None.

            Returns:
                Response: The HTTP response object.

        """
        response = None
        url = RequestHelper.gen_url_with_key(
            url=url, privateKey=self.api_key)
        try:
            response = requests.post(url,
                                     headers=self.DEFAULT_HEADERS,
                                     json=data)
            response.raise_for_status()
        except RequestException as e:
            return RequestHelper.handle_exception(response, e)
        return response

    def patchRequest(self,
                     url: str,
                     data: dict) -> Response:
        """
            Send an HTTP PATCH request to the specified URL with JSON data.

            Args:
                url (str): The URL to send the PATCH request to.
                data (dict): The JSON data to include in the request body.
                headers (Optional[Dict[str, str]]): Custom headers to include in the request. Defaults to None.

            Returns:
                Response: The HTTP response object.

        """
        response = None
        url = RequestHelper.gen_url_with_key(
            url=url, privateKey=self.api_key)
        try:
            response = requests.patch(url,
                                      headers=self.DEFAULT_HEADERS,
                                      json=data)
            response.raise_for_status()
        except RequestException as e:
            return RequestHelper.handle_exception(response, e)
        return response

    def deleteRequest(self, url: str) -> Response:
        """
            Send an HTTP DELETE request to the specified URL.

            Args:
                url (str): The URL to send the DELETE request to.
                headers (Optional[Dict[str, str]]): Custom headers to include in the request. Defaults to None.

            Returns:
                Response: The HTTP response object.

        """
        response = None
        url = RequestHelper.gen_url_with_key(
            url=url, privateKey=self.api_key)
        try:
            response = requests.delete(url, headers=self.DEFAULT_HEADERS)
            response.raise_for_status()
        except RequestException as e:
            return RequestHelper.handle_exception(response, e)
        return response

    @staticmethod
    def to_payload_static(dataclass_obj: Any, api_payload: Optional[List[str]] = None) -> Dict[str, Any]:
        """
            Convert a data class object to a dictionary payload.

            This method converts a data class object into a dictionary, optionally filtering keys.

            Args:
                dataclass_obj (Any): The data class object to be converted.
                keys (Optional[List[str]]): List of keys to include in the payload. If None, all keys with non-None values are included.

            Returns:
                Dict[str, Any]: The dictionary payload.

        """
        dataclass_dict = vars(dataclass_obj)
        filtered_dict = {key: value for key, value in dataclass_dict.items(
        ) if value and (api_payload is None or key in api_payload)}

        # Serialize using the custom serializer
        final_dict = json.loads(json.dumps(
            filtered_dict, default=FileHelper.json_default))
        return final_dict

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
        if response:
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


class FileHelper:
    @staticmethod
    def json_default(value):
        if isinstance(value, datetime.date):
            return dict(year=value.year, month=value.month, day=value.day)
        elif isinstance(value, list):
            return [FileHelper.json_default(item) for item in value]
        else:
            return value.__dict__

    def to_json(self, filepath: str):
        FileHelper.check_filepath(filepath)
        with open(file=filepath, mode='w', encoding='utf-8') as f:
            json.dump(self, f,
                      ensure_ascii=False, indent=4, default=FileHelper.json_default)

    @staticmethod
    def from_json(filepath: str):
        if not FileHelper.file_exists(filepath):
            raise ValueError(f'{filepath} does not exist')
        with open(file=filepath, mode='r') as json_file:
            return json.load(json_file)

    @staticmethod
    def check_filepath(filepath: str):
        if not os.path.exists(os.path.dirname(filepath)) and len(os.path.dirname(filepath)) > 0:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)

    @staticmethod
    def split_filepath(fullfilepath):
        p = pathlib.Path(fullfilepath)
        file_path = str(p.parent)+'/'
        file_name = p.name
        file_extension = ''
        for suffix in p.suffixes:
            file_name = file_name.replace(suffix, '')
            file_extension = file_extension+suffix
        return file_path, file_name, file_extension

    @staticmethod
    def file_exists(fullfilepath):
        file_path, file_name, file_extension = FileHelper.split_filepath(
            fullfilepath)

        if len(glob(f"{file_path}{file_name }-*{file_extension}") + glob(f"{file_path}{file_name}{file_extension}")) > 0:
            return True
        return False


class ListHelper:

    @staticmethod
    def chunk_list(lst, n):
        return [lst[i:i + n] for i in range(0, len(lst), n)]

    @staticmethod
    def convert_list(val):
        if isinstance(val, str):
            return ast.literal_eval(val)
        else:
            return val

    @staticmethod
    def compare_list(list1: List[str], list2: List[str]):
        return set(list1) == set(list2)

    @staticmethod
    def merge_list(lst1: List, lst2: Optional[Union[int, str, List]] = None) -> List:
        if isinstance(lst2, str) or isinstance(lst2, int):
            lst2 = [lst2]
        if not lst2:
            lst2 = []
        lst = list(filter(None, list(dict.fromkeys(lst1+lst2))))
        lst.sort()
        return lst



from .Import import Import, ImportList, Segment, SegmentList
from .Query import Query, QueryList
from .User import Identify
from .Cohort import Cohort, CohortList
TAGS = ['#automatic', '#imports']


@dataclass
class Workspace():
    """
    Dataclass for the Workspace entity in the Permutive ecosystem.
    """
    name: str
    organizationID: str
    workspaceID: str
    privateKey: str

    @property
    def isTopLevel(self):
        if self.organizationID == self.workspaceID:
            return True
        return False

    def list_cohorts(self,
                     include_child_workspaces: bool = False) -> CohortList:
        return Cohort.list(include_child_workspaces=include_child_workspaces,
                           privateKey=self.privateKey)

    def list_imports(self) -> List[Import]:
        return Import.list(privateKey=self.privateKey)

    def list_segments(self, import_id: str) -> List[Segment]:
        return Segment.list(import_id=import_id, privateKey=self.privateKey)

    def sync_imports_cohorts(self,
                             import_detail: 'Import',
                             prefix: Optional[str] = None,
                             inheritance: bool = False,
                             masterKey: Optional[str] = None):
        cohorts_list = self.list_cohorts(include_child_workspaces=True)
        for import_detail in Import.list(privateKey=self.privateKey):
            if (inheritance and import_detail.inheritance) or (not inheritance and not import_detail.inheritance):
                self.sync_import_cohorts(import_detail=import_detail,
                                         prefix=prefix,
                                         cohorts_list=cohorts_list,
                                         masterKey=masterKey)

    def sync_import_cohorts(self,
                            import_detail: Import,
                            prefix: Optional[str] = None,
                            cohorts_list: Optional[CohortList] = None,
                            masterKey: Optional[str] = None):
        import_segments = Segment.list(import_id=import_detail.id,
                                       privateKey=self.privateKey)
        if not import_segments:
            logging.warning("Import has no segment")
            return
        if not cohorts_list:
            cohorts_list = Cohort.list(include_child_workspaces=True,
                                       privateKey=self.privateKey)
        api_key = masterKey if masterKey is not None else self.privateKey
        cohort_tags = ListHelper.merge_list(TAGS, import_detail.name)
        provider_query = Query(name=f"{prefix or ''}{import_detail.name}",
                                    tags=cohort_tags,
                                    second_party_segments=[])

        provider_cohort = cohorts_list.name_dictionary.get(provider_query.name)

        provider_query.id = provider_cohort.id if provider_cohort is not None else None

        if provider_cohort:
            if provider_query.tags:
                provider_query.tags = ListHelper.merge_list(
                    provider_query.tags, provider_cohort.tags)
            else:
                provider_query.tags = provider_cohort.tags
        for import_segment in import_segments:
            logging.debug(
                f"AudienceAPI::sync_cohort::{import_detail.name}::{import_segment.name}")
            t_segment = (import_detail.code, import_segment.code)

            import_segment_query = Query(name=f"{prefix or ''}{import_detail.name} | {import_segment.name}",
                                         description=f'{import_detail.name} ({import_detail.id})::{import_segment.code}::{import_segment.name} ({import_segment.id})',
                                         tags=cohort_tags,
                                         second_party_segments=[t_segment],
                                         workspace_id=self.workspaceID)
            import_segment_cohort = cohorts_list.name_dictionary.get(
                import_segment_query .name)
            import_segment_query.id = import_segment_cohort.id if import_segment_cohort is not None else None

            if import_segment_cohort:
                if import_segment_query.tags:
                    import_segment_query.tags = ListHelper.merge_list(
                        import_segment_query.tags, import_segment_cohort.tags)
                else:
                    import_segment_query.tags = import_segment_cohort.tags
            import_segment_query.sync(api_key=api_key)
            if not provider_query.second_party_segments:
                provider_query.second_party_segments = []
            provider_query.second_party_segments.append(t_segment)
        provider_query.sync(api_key=api_key)

    def sync_imports_segments(self):
        cohorts_list = Cohort.list(include_child_workspaces=True,
                                   privateKey=self.privateKey)
        for item in Import.list(privateKey=self.privateKey):
            self.sync_import_cohorts(import_detail=item,
                                     prefix=f"{self.name} | Import | ",
                                     cohorts_list=cohorts_list)


@dataclass
class WorkspaceList(List[Workspace]):
    # Cache for each dictionary to avoid rebuilding
    _id_dictionary_cache: Dict[str, Workspace] = field(
        default_factory=dict, init=False)
    _name_dictionary_cache: Dict[str, Workspace] = field(
        default_factory=dict, init=False)

    def __init__(self, workspaces: Optional[List[Workspace]] = None):
        """Initializes the WorkspaceList with an optional list of Workspace objects."""
        super().__init__(workspaces if workspaces is not None else [])
        self.rebuild_cache()

    def rebuild_cache(self):
        """Rebuilds all caches based on the current state of the list."""
        self._id_dictionary_cache = {
            workspace.workspaceID: workspace for workspace in self if workspace.workspaceID}
        self._name_dictionary_cache = {
            workspace.name: workspace for workspace in self if workspace.name}

    @property
    def id_dictionary(self) -> Dict[str, Workspace]:
        """Returns a dictionary of workspaces indexed by their IDs."""
        if not self._id_dictionary_cache:
            self.rebuild_cache()
        return self._id_dictionary_cache

    @property
    def name_dictionary(self) -> Dict[str, Workspace]:
        """Returns a dictionary of workspaces indexed by their names."""
        if not self._name_dictionary_cache:
            self.rebuild_cache()
        return self._name_dictionary_cache

    def sync_imports_segments(self):
        """Syncs imports and segments for each workspace in the list."""
        for ws in self:
            ws.sync_imports_segments()

    @property
    def Masterworkspace(self) -> Workspace:
        """Returns the top-level workspace."""
        for workspace in self:
            if workspace.isTopLevel:
                return workspace
        raise ValueError("No Top-Level Workspace found")

    def to_json(self, filepath: str):
        """Saves the WorkspaceList to a JSON file at the specified filepath."""
        FileHelper.check_filepath(filepath)
        with open(file=filepath, mode='w', encoding='utf-8') as f:
            json.dump(self, f, ensure_ascii=False, indent=4,
                      default=FileHelper.json_default)

    @staticmethod
    def from_json(filepath: Optional[str] = None) -> 'WorkspaceList':
        """Creates a new WorkspaceList from a JSON file at the specified filepath."""
        if not filepath:
            filepath = os.environ.get("PERMUTIVE_APPLICATION_CREDENTIALS")
        if not filepath:
            raise ValueError(
                'Unable to get PERMUTIVE_APPLICATION_CREDENTIALS from .env')

        workspace_list = FileHelper.from_json(filepath)
        return WorkspaceList([Workspace(**workspace) for workspace in workspace_list])




__all__ = ["Cohort", "CohortList", "Import", "ImportList", "Segment", "SegmentList",
           "Workspace", "WorkspaceList", "Query", "QueryList", "Identify"]
