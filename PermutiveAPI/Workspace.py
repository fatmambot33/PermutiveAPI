"""Workspace utilities for interacting with the Permutive API."""

from typing import Callable, Dict, List, Optional, Type, Union, Any
from dataclasses import dataclass
from pathlib import Path
from PermutiveAPI.Utils import JSONSerializable, load_json_list
from PermutiveAPI.Audience.Import import Import, ImportList
from PermutiveAPI.Audience.Segment import Segment, SegmentList
from PermutiveAPI.Cohort import Cohort, CohortList


@dataclass
class Workspace(JSONSerializable[Dict[str, Any]]):
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
    list_cohorts()
        Retrieve a cached list of cohorts for the workspace.
    list_imports()
        Retrieve a cached list of imports for the workspace.
    list_segments()
        Retrieve a cached list of segments for a given import.
    """

    name: str
    organisation_id: str
    workspace_id: str
    api_key: str

    def __post_init__(self):
        """Initialise caches."""
        self._segment_cache: Dict[str, "SegmentList"] = {}

    @property
    def is_top_level(self) -> bool:
        """Determine if the workspace is the top-level workspace.

        Returns
        -------
        bool
            ``True`` if this workspace is top-level, otherwise ``False``.
        """
        return self.organisation_id == self.workspace_id

    def _get_or_refresh_cached_attribute(
        self, cache_attr: str, refresh_func: Callable[[], Any], force_refresh: bool
    ) -> Any:
        """Get a cached attribute or refresh it."""
        if force_refresh or not hasattr(self, cache_attr):
            refresh_func()
        return getattr(self, cache_attr)

    def _refresh_cohort_cache(self) -> CohortList:
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

    def list_cohorts(self, force_refresh: bool = False) -> CohortList:
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
        return self._get_or_refresh_cached_attribute(
            "_cohort_cache", self._refresh_cohort_cache, force_refresh
        )

    def _refresh_import_cache(self) -> "ImportList":
        """Re-fetch imports from the API and update the cache.

        Returns
        -------
        ImportList
            Updated list of imports.
        """
        self._import_cache = Import.list(api_key=self.api_key)
        return self._import_cache

    def list_imports(self, force_refresh: bool = False) -> "ImportList":
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
        return self._get_or_refresh_cached_attribute(
            "_import_cache", self._refresh_import_cache, force_refresh
        )

    def _refresh_segment_cache(self, import_id: str) -> None:
        """Re-fetch segments from the API and update the cache."""
        self._segment_cache[import_id] = Segment.list(
            import_id=import_id, api_key=self.api_key
        )

    def list_segments(self, import_id: str, force_refresh: bool = False) -> "SegmentList":
        """Retrieve a cached list of segments for a given import.

        Parameters
        ----------
        import_id : str
            The ID of the import to retrieve segments for.
        force_refresh : bool, optional
            Re-fetch the segment list if ``True``. Defaults to ``False``.

        Returns
        -------
        SegmentList
            Cached list of segments.
        """
        if force_refresh or import_id not in self._segment_cache:
            self._refresh_segment_cache(import_id)
        return self._segment_cache[import_id]


class WorkspaceList(List[Workspace], JSONSerializable[List[Any]]):
    """Manage a collection of Workspace objects.

    Methods
    -------
    from_json()
        Deserialize a list of workspaces from various JSON representations.
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
        self._refresh_cache()

    def _refresh_cache(self):
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
            self._refresh_cache()
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
            self._refresh_cache()
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
