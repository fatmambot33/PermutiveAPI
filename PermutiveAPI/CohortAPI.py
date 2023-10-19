import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Union, Any


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

        def to_payload(self, keys: Optional[List[str]] = ["name", "query", "description", "tags"]) -> Dict[str, Any]:
            return APIRequestHandler.to_payload(self, keys=keys)

        def to_json(self, filepath: str):
            FileHelper.to_json(self, filepath=filepath)

        @staticmethod
        def from_json(filepath: str) -> 'CohortAPI.Cohort':
            jsonObj = FileHelper.from_json(filepath=filepath)
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
        return [CohortAPI.Cohort(**cohort) for cohort in response.json()]

    def get(self, cohort_id: str) -> Cohort:
        """
        Fetches a specific cohort from the API using its ID.

        :param cohort_id: ID of the cohort.
        :return: Cohort object or None if not found.
        """
        logging.info(f"CohortAPI::get::{cohort_id}")
        url = f"{COHORT_API_ENDPOINT}{cohort_id}?k={self.__api_key}"
        response = APIRequestHandler.get(url=url)

        return CohortAPI.Cohort(**response.json())

    def get_by_name(self, cohort_name: str) -> Optional[Cohort]:
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

    def get_by_code(self, cohort_code: Union[int, str]) -> Optional[Cohort]:
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

    def create(self, cohort: Cohort) -> Cohort:
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

        return CohortAPI.Cohort(**response.json())

    def update(self, cohort: Cohort) -> Cohort:
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