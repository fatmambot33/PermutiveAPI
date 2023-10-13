import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Union

from . import WORKSPACES
from .APIRequestHandler import APIRequestHandler
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

    @dataclass
    class Cohort:
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

        def to_payload(self, keys: Optional[List[str]] = None) -> Dict[str, any]:
            return APIRequestHandler.to_payload(self, keys=keys)

        def to_file(self, filepath: str):
            FileHelper.save_to_json(self, filepath=filepath)

        def create(self):
            if self.workspace_id is None:
                raise ValueError("Missing workspace_id")
            api_key = WORKSPACES.get_privateKey(self.workspace_id)
            if api_key is None:
                raise ValueError("Missing api_key for the workspace")
            self = CohortAPI(api_key=api_key).create(self)

        def update(self):
            if self.workspace_id is None:
                raise ValueError("Missing workspace_id")
            api_key = WORKSPACES.get_privateKey(self.workspace_id)
            if api_key is None:
                raise ValueError("Missing api_key for the workspace")
            self = CohortAPI(api_key=api_key).update(self)

        def delete(self):
            if self.workspace_id is None:
                raise ValueError("Missing workspace_id")
            api_key = WORKSPACES.get_privateKey(self.workspace_id)
            if api_key is None:
                raise ValueError("Missing api_key for the workspace")
            self = CohortAPI(api_key=api_key).update(self)

        @staticmethod
        def from_file(filepath: str):
            jsonObj = FileHelper.read_json(filepath=filepath)
            return CohortAPI.Cohort(**jsonObj)

    def list(self, include_child_workspaces=False) -> List[Cohort]:
        """
        Fetches all cohorts from the API.

        :return: List of all cohorts.
        """
        logging.info(f"CohortAPI::list")
        url = f"{COHORT_API_ENDPOINT}?k={self.__api_key}"
        if include_child_workspaces:
            url = f"{url}&include-child-workspaces=true"
        response = APIRequestHandler.get(url=url)
        cohort_data = response.json()
        return [CohortAPI.Cohort(**cohort) for cohort in cohort_data]

    def get(self, cohort_id: str) -> Optional[Cohort]:
        """
        Fetches a specific cohort from the API using its ID.

        :param cohort_id: ID of the cohort.
        :return: Cohort object or None if not found.
        """
        logging.info(f"CohortAPI::get::{cohort_id}")
        url = f"{COHORT_API_ENDPOINT}{cohort_id}?k={self.__api_key}"
        response = APIRequestHandler.get(url=url)
        if response is None:
            return None
        if response.status_code == 200:
            cohort_data = response.json()
            return CohortAPI.Cohort(**cohort_data)

        return None

    def get_by_name(self, cohort_name: str) -> Optional[Cohort]:
        '''
        Object Oriented Permutive Cohort DETAIL API Call for https://api.permutive.app/cohorts-api/v2/cohorts
        :rtype: Cohort object
        :param cohort_id: str Cohort UUID. Required
        :return: Cohort object
        '''
        logging.info(f"CohortAPI::get_by_name::{cohort_name}")
        for cohort in self.list():
            if cohort_name == cohort.name and cohort.id is not None:
                return self.get(cohort.id)
        return None

    def get_by_code(self, cohort_code: Union[int, str]) -> Optional[Cohort]:
        '''
        Object Oriented Permutive Cohort DETAIL API Call for https://api.permutive.app/cohorts-api/v2/cohorts
        :rtype: Cohort object
        :param cohort_id: str Cohort UUID. Required
        :return: Cohort object
        '''
        if type(cohort_code) == str:
            cohort_code = int(cohort_code)
        logging.info(f"CohortAPI::get_by_code::{cohort_code}")
        for cohort in self.list():
            if cohort_code == cohort.code and cohort.id is not None:
                return self.get(cohort.id)

    def create(self, cohort: Cohort) -> Optional[Cohort]:
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
        data = cohort.to_payload(keys=["name", "query", "description", "tags"])
        response = APIRequestHandler.post(
            url=url,
            data=data)
        if response is not None:
            return CohortAPI.Cohort(**response.json())

    def update(self, cohort: Cohort) -> Optional[Cohort]:
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
        response = APIRequestHandler.patch(
            url=url,
            data=cohort.to_payload(keys=["name", "query", "description", "tags"]))
        if response is None:
            return response
        return CohortAPI.Cohort(**response.json())

    def delete(self, cohort_id: str) -> None:
        """
        Deletes a specific cohort.
        :param cohort_id: ID of the cohort to be deleted.
        :return: None
        """
        logging.info(f"CohortAPI::delete::{cohort_id}")
        url = f"{COHORT_API_ENDPOINT}{cohort_id}?k={self.__api_key}"
        APIRequestHandler.delete(url=url)

    def copy(self, cohort_id: str, k2: Optional[str] = None) -> Cohort:
        """
        Meant for copying a cohort
        :param cohort_id: str the cohort's id to duplicat. Required
        :param k2: str the key to use for creating the copy. Optional. If not specified, uses the current workspace API key
        :return: Response
        :rtype: Response
        """
        logging.info(f"CohortAPI::copy::{cohort.name}")
        if cohort_id is None:
            raise ValueError("cohort_id must be specified")
        cohort = self.get(cohort_id)
        cohort.id = None
        cohort.code = None
        cohort.name = cohort.name + ' (copy)'
        new_description = "Copy of " + cohort_id
        if cohort.description is not None:
            cohort.description = cohort.description + new_description
        else:
            cohort.description = new_description
        if k2 is None:
            return self.create(cohort)
        else:
            api = CohortAPI(k=k2)
            return api.create(cohort)
