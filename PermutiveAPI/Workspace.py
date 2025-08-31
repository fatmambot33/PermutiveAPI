"""Workspace utilities for interacting with the Permutive API."""

from typing import Dict, List, Optional, Type, Union
from dataclasses import dataclass
from pathlib import Path
from PermutiveAPI.Utils import JSONSerializable, load_json_list
from PermutiveAPI.Audience.Import import Import, ImportList
from PermutiveAPI.Audience.Segment import Segment
from PermutiveAPI.Cohort import Cohort, CohortList


@dataclass
class Workspace(JSONSerializable):
    """Represents a Workspace in the Permutive ecosystem.

    Parameters
    ----------
    name : str
        The name of the workspace.
    organisation_id : str
        The ID of the organization the workspace belongs to.
    workspace_id : str
        The ID of the workspace.
    api_key : str
        The API key for authentication.

    Methods
    -------
    is_top_level()
        Determine if the workspace is the top-level workspace.
    refresh_cohorts()
        Re-fetch cohorts from the API and update the cache.
    cohorts()
        Retrieve a cached list of cohorts for the workspace.
    list_cohorts()
        Retrieve a list of cohorts for the workspace.
    refresh_imports()
        Re-fetch imports from the API and update the cache.
    imports()
        Retrieve a cached list of imports for the workspace.
    list_segments()
        Retrieve a list of segments for a given import.
    """

    name: str
    organisation_id: str
    workspace_id: str
    api_key: str

    @property
    def is_top_level(self) -> bool:
        """Determine if the workspace is the top-level workspace.

        Returns
        -------
        bool
            ``True`` if this workspace is top-level, otherwise ``False``.
        """
        return self.organisation_id == self.workspace_id

    def refresh_cohorts(self) -> CohortList:
        """Re-fetch cohorts from the API and update the cache.

        Returns
        -------
        CohortList
            Updated list of cohorts.
        """
        self._cohort_cache = Cohort.list(
            include_child_workspaces=False, api_key=self.api_key
        )
        return self._cohort_cache

    def cohorts(self, force_refresh: bool = False) -> CohortList:
        """Retrieve a cached list of cohorts for the workspace.

        Parameters
        ----------
        force_refresh : bool, optional
            Re-fetch the cohort list if ``True``. Defaults to ``False``.

        Returns
        -------
        CohortList
            Cached list of cohorts.
        """
        if force_refresh or not hasattr(self, "_cohort_cache"):
            self.refresh_cohorts()
        return self._cohort_cache

    def list_cohorts(self, include_child_workspaces: bool = False) -> CohortList:
        """Retrieve a list of cohorts for the workspace.

        Parameters
        ----------
        include_child_workspaces : bool, optional
            Whether to include cohorts from child workspaces. Defaults to False.

        Returns
        -------
        CohortList
            A list of cohorts.
        """
        return Cohort.list(
            include_child_workspaces=include_child_workspaces, api_key=self.api_key
        )

    def refresh_imports(self) -> "ImportList":
        """Re-fetch imports from the API and update the cache.

        Returns
        -------
        ImportList
            Updated list of imports.
        """
        self._import_cache = Import.list(api_key=self.api_key)
        return self._import_cache

    def imports(self, force_refresh: bool = False) -> "ImportList":
        """Retrieve a cached list of imports for the workspace.

        Parameters
        ----------
        force_refresh : bool, optional
            Re-fetch the import list if ``True``. Defaults to ``False``.

        Returns
        -------
        ImportList
            Cached list of imports.
        """
        if force_refresh or not hasattr(self, "_import_cache"):
            self.refresh_imports()
        return self._import_cache

    def list_segments(self, import_id: str) -> List[Segment]:
        """Retrieve a list of segments for a given import.

        Parameters
        ----------
        import_id : str
            The ID of the import to retrieve segments for.

        Returns
        -------
        List[Segment]
            A list of segments.
        """
        return Segment.list(import_id=import_id, api_key=self.api_key)


class WorkspaceList(List[Workspace], JSONSerializable):
    """Manage a collection of Workspace objects.

    Methods
    -------
    from_json()
        Deserialize a list of workspaces from various JSON representations.
    rebuild_cache()
        Rebuild all caches based on the current state of the list.
    id_dictionary()
        Return a dictionary of workspaces indexed by their IDs.
    name_dictionary()
        Return a dictionary of workspaces indexed by their names.
    master_workspace()
        Return the top-level workspace.
    """

    @classmethod
    def from_json(
        cls: Type["WorkspaceList"],
        data: Union[dict, List[dict], str, Path],
    ) -> "WorkspaceList":
        """Deserialize a list of workspaces from various JSON representations."""
        data_list = load_json_list(data, cls.__name__, "Workspace")
        return cls([Workspace.from_json(item) for item in data_list])

    def __init__(self, items_list: Optional[List[Workspace]] = None):
        """Initialize the WorkspaceList with an optional list of Workspace objects.

        Parameters
        ----------
        items_list : Optional[List[Workspace]], optional
            Workspace objects to initialize with. Defaults to None.
        """
        super().__init__(items_list if items_list is not None else [])
        self._id_dictionary_cache: Dict[str, Workspace] = {}
        self._name_dictionary_cache: Dict[str, Workspace] = {}
        self.rebuild_cache()

    def rebuild_cache(self):
        """Rebuild all caches based on the current state of the list."""
        self._id_dictionary_cache = {
            workspace.workspace_id: workspace
            for workspace in self
            if workspace.workspace_id
        }
        self._name_dictionary_cache = {
            workspace.name: workspace for workspace in self if workspace.name
        }

    @property
    def id_dictionary(self) -> Dict[str, Workspace]:
        """Return a dictionary of workspaces indexed by their IDs.

        Returns
        -------
        Dict[str, Workspace]
            Mapping of workspace IDs to ``Workspace`` objects.
        """
        if not self._id_dictionary_cache:
            self.rebuild_cache()
        return self._id_dictionary_cache

    @property
    def name_dictionary(self) -> Dict[str, Workspace]:
        """Return a dictionary of workspaces indexed by their names.

        Returns
        -------
        Dict[str, Workspace]
            Mapping of workspace names to ``Workspace`` objects.
        """
        if not self._name_dictionary_cache:
            self.rebuild_cache()
        return self._name_dictionary_cache

    @property
    def master_workspace(self) -> Workspace:
        """Return the top-level workspace.

        Returns
        -------
        Workspace
            The workspace marked as top-level.

        Raises
        ------
        ValueError
            If no top-level workspace is found.
        """
        for workspace in self:
            if workspace.is_top_level:
                return workspace
        raise ValueError("No top-level workspace found")
