
import logging
from typing import Dict, List, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime
from PermutiveAPI.Utils import RequestHelper, JSONSerializable
from collections import defaultdict

_API_VERSION = "v2"
_API_ENDPOINT = f'https://api.permutive.app/cohorts-api/{_API_VERSION}/cohorts/'
_API_PAYLOAD = ["id", "name", "query", "description", "tags"]


@dataclass
class Cohort(JSONSerializable):
    """
    Represents a cohort entity in the Permutive ecosystem.

    Attributes:
        name (str): The name of the cohort.
        id (str, optional): The unique identifier of the cohort.
        code (str, optional): The code associated with the cohort.
        query (Dict, optional): The query used to define the cohort.
        tags (List[str], optional): Tags associated with the cohort.
        description (str, optional): A description of the cohort.
        state (str, optional): The state of the cohort.
        segment_type (str, optional): The type of segment.
        live_audience_size (int, optional): The size of the live audience.
        created_at (datetime, optional): The creation date of the cohort.
        last_updated_at (datetime, optional): The last update date of the cohort.
        workspace_id (str, optional): The ID of the associated workspace.
        request_id (str, optional): The request ID associated with cohort operations.
        error (str, optional): An error message, if an error occurs during operations.

    Methods:
        create(api_key: str) -> None:
            Creates a new cohort in the Permutive ecosystem.

        update(api_key: str) -> Cohort:
            Updates an existing cohort.

        delete(api_key: str) -> None:
            Deletes the current cohort.

        get_by_id(id: str, api_key: str) -> Cohort:
            Retrieves a cohort by its unique identifier.

        get_by_name(name: str, api_key: str) -> Optional[Cohort]:
            Retrieves a cohort by its name.

        get_by_code(code: Union[int, str], api_key: str) -> Optional[Cohort]:
            Retrieves a cohort by its code.

        list(include_child_workspaces: bool = False, api_key: str) -> List[Cohort]:
            Retrieves a list of all cohorts.
    """

    name: str
    id: Optional[str] = None
    code: Optional[str] = None
    query: Optional[Dict] = None
    tags: Optional[List[str]] = None
    description: Optional[str] = None
    subject_entity_level: Optional[str] = None
    state: Optional[str] = None
    segment_type: Optional[str] = None
    live_audience_size: Optional[int] = 0
    created_at: Optional[datetime] = field(default_factory=datetime.now)
    last_updated_at: Optional[datetime] = field(default_factory=datetime.now)
    workspace_id: Optional[str] = None
    request_id: Optional[str] = None
    error: Optional[str] = None

    def create(self,
               api_key: str):
        """
        Creates a new cohort.

        :param cohort: Cohort to be created.
        :return: Created cohort object.
        """
        logging.debug(f"CohortAPI::create::{self.name}")
        if not self.query:
            raise ValueError('query must be specified')
        if self.id:
            logging.warning("id is specified")
        url = f"{_API_ENDPOINT}"
        response = RequestHelper.post_static(api_key=api_key,
                                             url=url,
                                             data=RequestHelper.to_payload_static(self,
                                                                                  _API_PAYLOAD))
        created = Cohort.from_json(response.json())
        self.id = created.id
        self.code = created.code

    def update(self,
               api_key: str):
        """
        Updates an existing cohort.

        :param cohort_id: ID of the cohort to be updated.
        :param updated_cohort: Updated cohort data.
        :return: Updated cohort object.
        """
        logging.debug(f"CohortAPI::update::{self.name}")
        if not self.id:
            raise ValueError("Cohort ID must be specified for update.")
        url = f"{_API_ENDPOINT}{self.id}"

        response = RequestHelper.patch_static(api_key=api_key,
                                              url=url,
                                              data=RequestHelper.to_payload_static(self, _API_PAYLOAD))

        return Cohort.from_json(response.json())

    def delete(self,
               api_key) -> None:
        """
        Deletes a specific cohort.
        :param cohort_id: ID of the cohort to be deleted.
        :return: None
        """
        logging.debug(f"{datetime.now()}::CohortAPI::delete::{self.name}")
        if not self.id:
            raise ValueError("Cohort ID must be specified for deletion.")
        url = f"{_API_ENDPOINT}{self.id}"
        RequestHelper.delete_static(api_key=api_key,
                                    url=url)

    @staticmethod
    def get_by_id(id: str,
                  api_key: str) -> 'Cohort':
        """
        Fetches a specific cohort from the API using its ID.

        :param cohort_id: ID of the cohort.
        :return: Cohort object or None if not found.
        """
        logging.debug(f"{datetime.now()}::CohortAPI::get::{id}")
        url = f"{_API_ENDPOINT}{id}"
        response = RequestHelper.get_static(api_key=api_key,
                                            url=url)

        return Cohort.from_json(response.json())

    @staticmethod
    def get_by_name(
        name: str,
        api_key: str
    ) -> Optional['Cohort']:
        '''
            Object Oriented Permutive Cohort seqrch
            :rtype: Cohort object
            :param cohort_name: str Cohort Name. Required
            :return: Cohort object
        '''
        logging.debug(f"{datetime.now()}::CohortAPI::get_by_name::{name}")

        for cohort in Cohort.list(include_child_workspaces=True,
                                  api_key=api_key):
            if name == cohort.name and cohort.id:
                return Cohort.get_by_id(id=cohort.id,
                                        api_key=api_key)

    @staticmethod
    def get_by_code(
            code: Union[int, str],
            api_key: str) -> Optional['Cohort']:
        '''
        Object Oriented Permutive Cohort seqrch
        :rtype: Cohort object
        :param cohort_code: Union[int, str] Cohort Code. Required
        :return: Cohort object
        '''
        logging.debug(f"{datetime.now()}::CohortAPI::get_by_code::{code}")
        for cohort in Cohort.list(include_child_workspaces=True,
                                  api_key=api_key):
            if code == cohort.code and cohort.id:
                return Cohort.get_by_id(id=cohort.id,
                                        api_key=api_key)

    @staticmethod
    def list(api_key: str,
             include_child_workspaces=False) -> 'CohortList':
        """
            Fetches all cohorts from the API.

            :return: List of all cohorts.
        """
        logging.debug(f"CohortAPI::list")

        url = RequestHelper.generate_url_with_key(url=_API_ENDPOINT,
                                                  api_key=api_key)
        if include_child_workspaces:
            url = f"{url}&include-child-workspaces=true"

        response = RequestHelper.get_static(api_key, url)
        cohort_list = CohortList([Cohort(**cohort)
                                 for cohort in response.json()])
        return cohort_list


class CohortList(List[Cohort], JSONSerializable):

    def __init__(self, items_list: Optional[List[Cohort]] = None):
        super().__init__(items_list if items_list is not None else [])
        self._id_dictionary_cache: Dict[str, Cohort] = {}
        self._code_dictionary_cache: Dict[str, Cohort] = {}
        self._name_dictionary_cache: Dict[str, Cohort] = {}
        self._tag_dictionary_cache: Dict[str, List[Cohort]] = defaultdict(list)
        self._workspace_dictionary_cache: Dict[str, List[Cohort]] = defaultdict(
            list)
        self._segment_type_dictionary_cache: Dict[str, List[Cohort]] = defaultdict(
            list)
        self.rebuild_cache()

    def rebuild_cache(self):
        """Rebuilds all caches based on the current state of the list."""
        self._id_dictionary_cache = {
            cohort.id: cohort for cohort in self if cohort.id}
        self._code_dictionary_cache = {
            cohort.code: cohort for cohort in self if cohort.code}
        self._name_dictionary_cache = {
            cohort.name: cohort for cohort in self if cohort.name}

        for cohort in self:
            if cohort.tags:
                for tag in cohort.tags:
                    self._tag_dictionary_cache[tag].append(cohort)
            if cohort.segment_type:
                self._segment_type_dictionary_cache[cohort.segment_type].append(
                    cohort)
            if cohort.workspace_id:
                self._workspace_dictionary_cache[cohort.workspace_id].append(
                    cohort)

    @property
    def id_dictionary(self) -> Dict[str, Cohort]:
        """Returns a dictionary of cohorts indexed by their IDs."""
        if not self._id_dictionary_cache:
            self.rebuild_cache()
        return self._id_dictionary_cache

    @property
    def code_dictionary(self) -> Dict[str, Cohort]:
        """Returns a dictionary of cohorts indexed by their code"""
        if not self._code_dictionary_cache:
            self.rebuild_cache()
        return self._id_dictionary_cache

    @property
    def name_dictionary(self) -> Dict[str, Cohort]:
        """Returns a dictionary of cohorts indexed by their names."""
        if not self._name_dictionary_cache:
            self.rebuild_cache()
        return self._name_dictionary_cache

    @property
    def tag_dictionary(self) -> Dict[str, List[Cohort]]:
        """Returns a dictionary of cohorts indexed by their tags."""
        if not self._tag_dictionary_cache:
            self.rebuild_cache()
        return self._tag_dictionary_cache

    @property
    def segment_type_dictionary(self) -> Dict[str, List[Cohort]]:
        """Returns a dictionary of cohorts indexed by their tags."""
        if not self._segment_type_dictionary_cache:
            self.rebuild_cache()
        return self._segment_type_dictionary_cache

    @property
    def workspace_dictionary(self) -> Dict[str, List[Cohort]]:
        """Returns a dictionary of cohorts indexed by their workspace IDs."""
        if not self._workspace_dictionary_cache:
            self.rebuild_cache()
        return self._workspace_dictionary_cache

    def to_list(self) -> List[Cohort]:
        """Returns the list of cohorts."""
        return list(self)
