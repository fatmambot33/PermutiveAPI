"""Workspace utilities for interacting with the Permutive API."""

import json
from typing import Dict, List, Optional, Type, Union
from dataclasses import dataclass
from pathlib import Path
from PermutiveAPI.Utils import JSONSerializable
from PermutiveAPI.Audience.Import import Import, ImportList
from PermutiveAPI.Audience.Segment import Segment
from PermutiveAPI.Cohort import Cohort, CohortList


@dataclass
class Workspace(JSONSerializable):
    """Represents a Workspace in the Permutive ecosystem.

    Attributes
    ----------
    name : str
        The name of the workspace.
    organisation_id : str
        The ID of the organization the workspace belongs to.
    workspace_id : str
        The ID of the workspace.
    api_key : str
        The API key for authentication.
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

    @property
    def cohorts(self) -> CohortList:
        """Retrieve a cached list of cohorts for the workspace.

        Returns
        -------
        CohortList
            Cached list of cohorts.
        """
        if not hasattr(self, "_cohort_cache"):
            self._cohort_cache = Cohort.list(
                include_child_workspaces=False, api_key=self.api_key
            )
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

    @property
    def imports(self) -> "ImportList":
        """Retrieve a cached list of imports for the workspace.

        Returns
        -------
        ImportList
            Cached list of imports.
        """
        if not hasattr(self, "_import_cache"):
            self._import_cache = Import.list(api_key=self.api_key)
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
    """Manage a collection of Workspace objects."""

    @classmethod
    def from_json(
        cls: Type["WorkspaceList"],
        data: Union[dict, List[dict], str, Path],
    ) -> "WorkspaceList":
        """Deserialize a list of workspaces from various JSON representations."""
        if isinstance(data, dict):
            raise TypeError(
                f"Cannot create a {cls.__name__} from a dictionary. Use from_json on the Workspace class for single objects."
            )
        if isinstance(data, (str, Path)):
            try:
                if isinstance(data, Path):
                    content = data.read_text(encoding="utf-8")
                else:
                    content = data
                loaded_data = json.loads(content)
                if not isinstance(loaded_data, list):
                    raise TypeError(
                        f"JSON content from {type(data).__name__} did not decode to a list."
                    )
                data = loaded_data
            except Exception as e:
                raise TypeError(f"Failed to parse JSON from input: {e}")

        if isinstance(data, list):
            return cls([Workspace.from_json(item) for item in data])

        raise TypeError(
            f"`from_json()` expected a list of dicts, JSON string, or Path, but got {type(data).__name__}"
        )

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
        raise ValueError("No Top-Level Workspace found")
