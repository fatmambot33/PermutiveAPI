"""Cohort management utilities for the Permutive API."""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Union, Type, Any
from dataclasses import dataclass, field
from datetime import datetime, timezone
from PermutiveAPI.Utils import RequestHelper, JSONSerializable, load_json_list
from collections import defaultdict

_API_VERSION = "v2"
_API_ENDPOINT = f"https://api.permutive.app/cohorts-api/{_API_VERSION}/cohorts/"
_API_PAYLOAD = ["id", "name", "query", "description", "tags"]


@dataclass
class Cohort(JSONSerializable[Dict[str, Any]]):
    """Represents a cohort entity in the Permutive ecosystem.

    Parameters
    ----------
    name : str
        The name of the cohort.
    id : Optional[str], optional
        The unique identifier of the cohort (default: None).
    code : Optional[str], optional
        The code associated with the cohort (default: None).
    query : Optional[Dict], optional
        The query used to define the cohort (default: None).
    tags : Optional[List[str]], optional
        Tags associated with the cohort (default: None).
    description : Optional[str], optional
        A description of the cohort (default: None).
    subject_entity_level : Optional[str], optional
        The entity level for the cohort's subjects (default: None).
    state : Optional[str], optional
        The state of the cohort (default: None).
    segment_type : Optional[str], optional
        The type of segment (default: None).
    live_audience_size : Optional[int], optional
        The size of the live audience (default: 0).
    created_at : Optional[datetime], optional
        The creation date of the cohort (default: `datetime.now(tz=timezone.utc)`).
    last_updated_at : Optional[datetime], optional
        The last update date of the cohort (default: `datetime.now(tz=timezone.utc)`).
    workspace_id : Optional[str], optional
        The ID of the associated workspace (default: None).
    request_id : Optional[str], optional
        The request ID associated with cohort operations (default: None).
    error : Optional[str], optional
        An error message, if an error occurs during operations (default: None).

    Methods
    -------
    create(api_key)
        Create a new cohort in Permutive.
    update(api_key)
        Update an existing cohort in Permutive.
    delete(api_key)
        Delete a cohort from Permutive.
    activate(api_key)
        Activate the cohort in Permutive.
    archive(api_key)
        Archive the cohort in Permutive.
    get_by_id(id, api_key)
        Fetch a specific cohort from the API using its ID.
    get_by_name(name, api_key)
        Retrieve a cohort by its name.
    get_by_code(code, api_key)
        Retrieve a cohort by its code.
    list(api_key, include_child_workspaces=False)
        Fetch all cohorts from the API.
    """

    _request_helper = RequestHelper

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
    created_at: Optional[datetime] = field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )
    last_updated_at: Optional[datetime] = field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )
    workspace_id: Optional[str] = None
    request_id: Optional[str] = None
    error: Optional[str] = None

    def __post_init__(self) -> None:
        """Normalize timestamps so they are consistent.

        If one of ``created_at`` or ``updated_at`` is missing, copy the other
        value. If both are missing, initialize both to the same current
        UTC timestamp. This avoids microsecond-level drift between the two
        fields and ensures deterministic equality/serialization.
        """
        if self.created_at is None and self.last_updated_at is None:
            now = datetime.now(tz=timezone.utc)
            self.created_at = now
            self.last_updated_at = now
        elif self.created_at is None:
            self.created_at = self.last_updated_at
        elif self.last_updated_at is None:
            self.last_updated_at = self.created_at

    def create(self, api_key: str) -> None:
        """Create a new cohort in Permutive.

        The method sends a POST request to the Permutive API to create a new cohort
        based on the instance's attributes. The ``id`` and ``code`` of the instance
        are updated with the values returned by the API.

        Parameters
        ----------
        api_key : str
            The API key for authentication.

        Returns
        -------
        None
        """
        logging.debug(f"CohortAPI::create::{self.name}")
        if not self.query:
            raise ValueError("query must be specified")
        if self.id:
            logging.warning("id is specified")
        url = f"{_API_ENDPOINT}"
        data = RequestHelper.to_payload_static(self, _API_PAYLOAD)
        response = self._request_helper.post_static(
            api_key=api_key,
            url=url,
            data=data,
        )
        if response is None:
            raise ValueError("Response is None")
        cohort = Cohort.from_json(response.json())
        if isinstance(cohort, Cohort):
            self.id = cohort.id
            self.code = cohort.code
            self.created_at = cohort.created_at
            self.last_updated_at = cohort.last_updated_at
            self.workspace_id = cohort.workspace_id
            self.request_id = cohort.request_id
        else:
            raise ValueError("Unable to create cohort")

    def update(self, api_key: str) -> None:
        """Update an existing cohort in Permutive.

        This method sends a PATCH request to the Permutive API to update a cohort.
        The cohort to be updated is identified by the ``id`` attribute of the
        instance.

        Parameters
        ----------
        api_key : str
            The API key for authentication.

        Returns
        -------
        None
        """
        logging.debug(f"CohortAPI::update::{self.name}")
        if not self.id:
            raise ValueError("Cohort ID must be specified for update.")
        url = f"{_API_ENDPOINT}{self.id}"
        data = RequestHelper.to_payload_static(self, _API_PAYLOAD)
        response = self._request_helper.patch_static(
            api_key=api_key,
            url=url,
            data=data,
        )
        if response is None:
            raise ValueError("Response is None")
        cohort = Cohort.from_json(response.json())
        self.last_updated_at = cohort.last_updated_at
        self.request_id = cohort.request_id

    def delete(self, api_key: str) -> None:
        """Delete a cohort from Permutive.

        This method sends a DELETE request to the Permutive API to delete the cohort
        identified by the ``id`` attribute of the instance.

        Parameters
        ----------
        api_key : str
            The API key for authentication.

        Returns
        -------
        None
        """
        logging.debug(f"CohortAPI::delete::{self.name}")
        if not self.id:
            raise ValueError("Cohort ID must be specified for deletion.")
        url = f"{_API_ENDPOINT}{self.id}"
        self._request_helper.delete_static(api_key=api_key, url=url)

    @staticmethod
    def get_by_id(id: str, api_key: str) -> "Cohort":
        """Fetch a specific cohort from the API using its ID.

        Parameters
        ----------
        id : str
            The ID of the cohort to retrieve.
        api_key : str
            The API key for authentication.

        Returns
        -------
        Cohort
            The cohort retrieved from the API.

        Raises
        ------
        ValueError
            If the cohort cannot be fetched.
        """
        logging.debug(f"CohortAPI::get::{id}")
        url = f"{_API_ENDPOINT}{id}"
        response = Cohort._request_helper.get_static(api_key=api_key, url=url)
        if response is None:
            raise ValueError("Response is None")
        return Cohort.from_json(response.json())

    @staticmethod
    def get_by_name(name: str, api_key: str) -> Optional["Cohort"]:
        """Retrieve a cohort by its name.

        This method searches for a cohort with the specified name.

        Parameters
        ----------
        name : str
            The name of the cohort to retrieve.
        api_key : str
            The API key for authentication.

        Returns
        -------
        Optional[Cohort]
            The matching cohort if found, otherwise ``None``.
        """
        logging.debug(f"CohortAPI::get_by_name::{name}")

        cohorts = Cohort.list(include_child_workspaces=True, api_key=api_key)
        cohort = cohorts.name_dictionary.get(name)
        if cohort is not None and cohort.id is not None:
            return Cohort.get_by_id(cohort.id, api_key)

    @staticmethod
    def get_by_code(code: Union[int, str], api_key: str) -> Optional["Cohort"]:
        """Retrieve a cohort by its code.

        This method searches for a cohort with the specified code.

        Parameters
        ----------
        code : Union[int, str]
            The code of the cohort to retrieve.
        api_key : str
            The API key for authentication.

        Returns
        -------
        Optional[Cohort]
            The matching cohort if found, otherwise ``None``.
        """
        logging.debug(f"CohortAPI::get_by_code::{code}")
        cohorts = Cohort.list(include_child_workspaces=True, api_key=api_key)
        cohort = cohorts.code_dictionary.get(str(code))
        if cohort is not None and cohort.id is not None:
            return Cohort.get_by_id(cohort.id, api_key)

    @staticmethod
    def list(api_key: str, include_child_workspaces: bool = False) -> "CohortList":
        """Fetch all cohorts from the API.

        Parameters
        ----------
        api_key : str
            The API key for authentication.
        include_child_workspaces : bool, optional
            Whether to include cohorts from child workspaces (default: False).

        Returns
        -------
        CohortList
            A list of all cohorts.
        """
        logging.debug(f"CohortAPI::list")

        url = _API_ENDPOINT
        if include_child_workspaces:
            url += "?include-child-workspaces=true"

        response = Cohort._request_helper.get_static(api_key, url)
        if response is None:
            raise ValueError("Response is None")
        return CohortList.from_json(response.json())


class CohortList(List[Cohort], JSONSerializable[List[Any]]):
    """A list-like object for managing a collection of Cohort instances.

    It provides caching mechanisms for quick lookups by id, code, name, etc.

    Methods
    -------
    from_json(data)
        Deserialize a list of cohorts from various JSON representations.
    rebuild_cache()
        Rebuild all caches based on the current state of the list.
    id_dictionary()
        Return a dictionary of cohorts indexed by their IDs.
    code_dictionary()
        Return a dictionary of cohorts indexed by their code.
    name_dictionary()
        Return a dictionary of cohorts indexed by their names.
    tag_dictionary()
        Return a dictionary of cohorts indexed by their tags.
    segment_type_dictionary()
        Return a dictionary of cohorts indexed by their tags.
    workspace_dictionary()
        Return a dictionary of cohorts indexed by their workspace IDs.
    """

    @classmethod
    def from_json(
        cls: Type["CohortList"],
        data: Union[dict, List[dict], str, Path],
    ) -> "CohortList":
        """Deserialize a list of cohorts from various JSON representations."""
        data_list = load_json_list(data, cls.__name__, "Cohort")
        return cls([Cohort.from_json(item) for item in data_list])

    def __init__(self, items_list: Optional[List[Cohort]] = None):
        """Initialize the CohortList.

        Parameters
        ----------
        items_list : Optional[List[Cohort]], optional
            Cohort objects to initialize with (default: None).
        """
        super().__init__(items_list if items_list is not None else [])
        self._id_dictionary_cache: Dict[str, Cohort] = {}
        self._code_dictionary_cache: Dict[str, Cohort] = {}
        self._name_dictionary_cache: Dict[str, Cohort] = {}
        self._tag_dictionary_cache: Dict[str, List[Cohort]] = defaultdict(list)
        self._workspace_dictionary_cache: Dict[str, List[Cohort]] = defaultdict(list)
        self._segment_type_dictionary_cache: Dict[str, List[Cohort]] = defaultdict(list)
        self.rebuild_cache()

    def rebuild_cache(self):
        """Rebuild all caches based on the current state of the list.

        Returns
        -------
        None
        """
        self._id_dictionary_cache = {cohort.id: cohort for cohort in self if cohort.id}
        self._code_dictionary_cache = {
            str(cohort.code): cohort for cohort in self if cohort.code
        }
        self._name_dictionary_cache = {
            cohort.name: cohort for cohort in self if cohort.name
        }

        self._tag_dictionary_cache = defaultdict(list)
        self._workspace_dictionary_cache = defaultdict(list)
        self._segment_type_dictionary_cache = defaultdict(list)

        for cohort in self:
            if cohort.tags:
                for tag in cohort.tags:
                    self._tag_dictionary_cache[tag].append(cohort)
            if cohort.segment_type:
                self._segment_type_dictionary_cache[cohort.segment_type].append(cohort)
            if cohort.workspace_id:
                self._workspace_dictionary_cache[cohort.workspace_id].append(cohort)

    @property
    def id_dictionary(self) -> Dict[str, Cohort]:
        """Return a dictionary of cohorts indexed by their IDs.

        Returns
        -------
        Dict[str, Cohort]
            A mapping of cohort IDs to ``Cohort`` instances.
        """
        if not self._id_dictionary_cache:
            self.rebuild_cache()
        return self._id_dictionary_cache

    @property
    def code_dictionary(self) -> Dict[str, Cohort]:
        """Return a dictionary of cohorts indexed by their code.

        Returns
        -------
        Dict[str, Cohort]
            A mapping of cohort codes to ``Cohort`` instances.
        """
        if not self._code_dictionary_cache:
            self.rebuild_cache()
        return self._code_dictionary_cache

    @property
    def name_dictionary(self) -> Dict[str, Cohort]:
        """Return a dictionary of cohorts indexed by their names.

        Returns
        -------
        Dict[str, Cohort]
            A mapping of cohort names to ``Cohort`` instances.
        """
        if not self._name_dictionary_cache:
            self.rebuild_cache()
        return self._name_dictionary_cache

    @property
    def tag_dictionary(self) -> Dict[str, List[Cohort]]:
        """Return a dictionary of cohorts indexed by their tags.

        Returns
        -------
        Dict[str, List[Cohort]]
            A mapping of tag names to lists of cohorts.
        """
        if not self._tag_dictionary_cache:
            self.rebuild_cache()
        return self._tag_dictionary_cache

    @property
    def segment_type_dictionary(self) -> Dict[str, List[Cohort]]:
        """Return a dictionary of cohorts indexed by their tags.

        Returns
        -------
        Dict[str, List[Cohort]]
            A mapping of segment types to lists of cohorts.
        """
        if not self._segment_type_dictionary_cache:
            self.rebuild_cache()
        return self._segment_type_dictionary_cache

    @property
    def workspace_dictionary(self) -> Dict[str, List[Cohort]]:
        """Return a dictionary of cohorts indexed by their workspace IDs.

        Returns
        -------
        Dict[str, List[Cohort]]
            A mapping of workspace IDs to lists of cohorts.
        """
        if not self._workspace_dictionary_cache:
            self.rebuild_cache()
        return self._workspace_dictionary_cache

    @staticmethod
    def get_cohorts(
        api_key: str, include_child_workspaces: bool = False
    ) -> "CohortList":
        """Fetch all cohorts from the API.

        Parameters
        ----------
        api_key : str
            The API key for authentication.
        include_child_workspaces : bool, optional
            Whether to include cohorts from child workspaces (default: False).

        Returns
        -------
        CohortList
            A list of all cohorts.
        """
        url = _API_ENDPOINT
        if include_child_workspaces:
            url += "?include-child-workspaces=true"

        response = Cohort._request_helper.get_static(api_key, url)
        if response is None:
            raise ValueError("Response is None")
        return CohortList.from_json(response.json())
