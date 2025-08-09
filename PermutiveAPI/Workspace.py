"""Workspace utilities for interacting with the Permutive API."""

from typing import Dict, List, Optional, Any, overload, Type, Union
from dataclasses import dataclass
from pathlib import Path
from PermutiveAPI.Utils import JSONSerializable
from PermutiveAPI.Import import Import, ImportList
from PermutiveAPI.Segment import Segment
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
    def isTopLevel(self) -> bool:
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
        if not hasattr(self, '_cohort_cache'):
            self._cohort_cache = Cohort.list(
                include_child_workspaces=False, api_key=self.api_key)
        return self._cohort_cache

    def list_cohorts(self,
                     include_child_workspaces: bool = False) -> CohortList:
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
        return Cohort.list(include_child_workspaces=include_child_workspaces,
                           api_key=self.api_key)

    @property
    def imports(self) -> "ImportList":
        """Retrieve a cached list of imports for the workspace.

        Returns
        -------
        ImportList
            Cached list of imports.
        """
        if not hasattr(self, '_import_cache'):
            self._import_cache = Import.list(api_key=self.api_key)
        return self._import_cache

    def list_segments(self,
                      import_id: str) -> List[Segment]:
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
        return Segment.list(import_id=import_id,
                            api_key=self.api_key)

    @overload
    @classmethod
    def from_json(cls: Type["Workspace"], data: dict) -> "Workspace": ...

    @overload
    @classmethod
    def from_json(cls: Type["Workspace"],
                  data: list[dict]) -> list["Workspace"]: ...

    @overload
    @classmethod
    def from_json(cls: Type["Workspace"], data: str) -> Union["Workspace",
                                                              list["Workspace"]]: ...

    @overload
    @classmethod
    def from_json(cls: Type["Workspace"], data: Path) -> Union["Workspace",
                                                               list["Workspace"]]: ...

    @classmethod
    def from_json(cls: Type["Workspace"], data: Any) -> Union["Workspace", list["Workspace"]]:
        """Deserialize workspace data from various JSON representations."""
        return super().from_json(data)


class WorkspaceList(List[Workspace], JSONSerializable):
    """Manage a collection of Workspace objects."""

    def __init__(self,
                 items_list: Optional[List[Workspace]] = None):
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

    @overload
    @classmethod
    def from_json(cls: Type["WorkspaceList"], data: dict) -> "WorkspaceList": ...

    @overload
    @classmethod
    def from_json(cls: Type["WorkspaceList"],
                  data: list[dict]) -> "WorkspaceList": ...

    @overload
    @classmethod
    def from_json(cls: Type["WorkspaceList"], data: str) -> "WorkspaceList": ...

    @overload
    @classmethod
    def from_json(cls: Type["WorkspaceList"], data: Path) -> "WorkspaceList": ...

    @classmethod
    def from_json(cls: Type["WorkspaceList"], data: Any) -> "WorkspaceList":
        """Deserialize workspace data from various JSON representations."""
        result = super().from_json(data)
        if isinstance(result, cls):
            return result
        # This should be dead code at runtime if my analysis is correct
        raise TypeError(f"Expected {cls.__name__}, got {type(result).__name__}")

    def rebuild_cache(self):
        """Rebuild all caches based on the current state of the list."""
        self._id_dictionary_cache = {
            workspace.workspace_id: workspace for workspace in self if workspace.workspace_id}
        self._name_dictionary_cache = {
            workspace.name: workspace for workspace in self if workspace.name}

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
            if workspace.isTopLevel:
                return workspace
        raise ValueError("No Top-Level Workspace found")
