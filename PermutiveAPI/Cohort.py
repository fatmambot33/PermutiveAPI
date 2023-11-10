import json
import logging
from typing import Dict, List, Optional, Union
from dataclasses import dataclass
from datetime import datetime
from .APIRequestHandler import APIRequestHandler
from .Utils import FileHelper


_API_VERSION = "v2"
_API_ENDPOINT = f'https://api.permutive.app/cohorts-api/{_API_VERSION}/cohorts/'
_API_PAYLOAD = ["id", "name", "query", "description", "tags"]


@dataclass
class Cohort():
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
        create(privateKey: str) -> None:
            Creates a new cohort in the Permutive ecosystem.

        update(privateKey: str) -> Cohort:
            Updates an existing cohort.

        delete(privateKey: str) -> None:
            Deletes the current cohort.

        get_by_id(id: str, privateKey: str) -> Cohort:
            Retrieves a cohort by its unique identifier.

        get_by_name(name: str, privateKey: str) -> Optional[Cohort]:
            Retrieves a cohort by its name.

        get_by_code(code: Union[int, str], privateKey: str) -> Optional[Cohort]:
            Retrieves a cohort by its code.

        list(include_child_workspaces: bool = False, privateKey: str) -> List[Cohort]:
            Retrieves a list of all cohorts.
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

    def create(self,
               privateKey: Optional[str] = None):
        """
        Creates a new cohort.

        :param cohort: Cohort to be created.
        :return: Created cohort object.
        """
        logging.debug(f"CohortAPI::create::{self.name}")
        if not privateKey:
            raise ValueError("privateKey must be specified")
        if not self.query:
            raise ValueError('query must be specified')
        if self.id:
            logging.warning("id is specified")
        url = f"{_API_ENDPOINT}"
        response = APIRequestHandler.postRequest_static(privateKey=privateKey,
                                                        url=url,
                                                        data=APIRequestHandler.to_payload_static(self, _API_PAYLOAD))
        created = Cohort(**response.json())
        self.id = created.id
        self.code = created.code

    def update(self,
               privateKey: Optional[str] = None):
        """
        Updates an existing cohort.

        :param cohort_id: ID of the cohort to be updated.
        :param updated_cohort: Updated cohort data.
        :return: Updated cohort object.
        """
        logging.debug(f"CohortAPI::update::{self.name}")
        if not privateKey:
            raise ValueError("privateKey must be specified")
        if not self.id:
            logging.warning("id must be specified")
        url = f"{_API_ENDPOINT}{self.id}"

        response = APIRequestHandler.patchRequest_static(privateKey=privateKey,
                                                         url=url,
                                                         data=APIRequestHandler.to_payload_static(self, _API_PAYLOAD))

        return Cohort(**response.json())

    def delete(self,
               privateKey: Optional[str] = None) -> None:
        """
        Deletes a specific cohort.
        :param cohort_id: ID of the cohort to be deleted.
        :return: None
        """
        logging.debug(f"CohortAPI::update::{self.name}")
        if not privateKey:
            raise ValueError("privateKey must be specified")
        if not self.id:
            logging.warning("id must be specified")
        url = f"{_API_ENDPOINT}{self.id}"
        APIRequestHandler.deleteRequest_static(privateKey=privateKey,
                                               url=url)

    @staticmethod
    def get_by_id(id: str,
                  privateKey: str) -> 'Cohort':
        """
        Fetches a specific cohort from the API using its ID.

        :param cohort_id: ID of the cohort.
        :return: Cohort object or None if not found.
        """
        logging.debug(f"CohortAPI::get::{id}")
        url = f"{_API_ENDPOINT}{id}"
        response = APIRequestHandler.getRequest_static(privateKey=privateKey,
                                                       url=url)

        return Cohort(**response.json())

    @staticmethod
    def get_by_name(
        name: str,
        privateKey: str
    ) -> Optional['Cohort']:
        '''
            Object Oriented Permutive Cohort seqrch
            :rtype: Cohort object
            :param cohort_name: str Cohort Name. Required
            :return: Cohort object
        '''
        logging.debug(f"CohortAPI::get_by_name::{name}")

        for cohort in Cohort.list(include_child_workspaces=True,
                                  privateKey=privateKey):
            if name == cohort.name and cohort.id:
                return Cohort.get_by_id(id=cohort.id,
                                        privateKey=privateKey)

    @staticmethod
    def get_by_code(
            code: Union[int, str],
            privateKey: str) -> Optional['Cohort']:
        '''
        Object Oriented Permutive Cohort seqrch
        :rtype: Cohort object
        :param cohort_code: Union[int, str] Cohort Code. Required
        :return: Cohort object
        '''
        if type(code) == str:
            code = int(code)
        logging.debug(f"CohortAPI::get_by_code::{code}")
        for cohort in Cohort.list(include_child_workspaces=True,
                                  privateKey=privateKey):
            if code == cohort.code and cohort.id:
                return Cohort.get_by_id(id=cohort.id,
                                        privateKey=privateKey)

    @staticmethod
    def list(include_child_workspaces=False,
             privateKey: Optional[str] = None) -> List['Cohort']:
        """
            Fetches all cohorts from the API.

            :return: List of all cohorts.
        """
        logging.debug(f"CohortAPI::list")

        if not privateKey:
            raise ValueError("No Private Key")

        url = APIRequestHandler.gen_url_with_key(_API_ENDPOINT, privateKey)
        if include_child_workspaces:
            url = f"{url}&include-child-workspaces=true"

        response = APIRequestHandler.getRequest_static(privateKey, url)
        return [Cohort(**cohort) for cohort in response.json()]

    def to_json(self, filepath: str):
        FileHelper.check_filepath(filepath)
        with open(file=filepath, mode='w', encoding='utf-8') as f:
            json.dump(self, f,
                      ensure_ascii=False, indent=4, default=FileHelper.json_default)

    @staticmethod
    def from_json(filepath: str)->'Cohort':
        if not FileHelper.file_exists(filepath):
            raise ValueError(f'{filepath} does not exist')
        with open(file=filepath, mode='r') as json_file:
            return Cohort(**json.load(json_file))
