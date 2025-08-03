"""Workspace utilities for interacting with the Permutive API."""

from typing import Dict, List, Optional, Any, overload, Type, Union
import json
from dataclasses import dataclass
from pathlib import Path
from PermutiveAPI.Utils import JSONSerializable
from PermutiveAPI.Import import Import, Segment
from PermutiveAPI.Cohort import Cohort, CohortList


@dataclass
class Workspace(JSONSerializable):
    """
    Represents a Workspace in the Permutive ecosystem.

    :param name: The name of the workspace.
    :type name: str
    :param organisation_id: The ID of the organization the workspace belongs to.
    :type organisation_id: str
    :param workspace_id: The ID of the workspace.
    :type workspace_id: str
    :param api_key: The API key for authentication.
    :type api_key: str
    """

    name: str
    organisation_id: str
    workspace_id: str
    api_key: str

    @property
    def isTopLevel(self) -> bool:
        """Determines if the workspace is the top-level workspace."""
        return self.organisation_id == self.workspace_id

    @property
    def cohorts(self) -> CohortList:
        """Retrieve a cached list of cohorts for the workspace."""
        if not hasattr(self, '_cohort_cache'):
            self._cohort_cache = Cohort.list(
                include_child_workspaces=False, api_key=self.api_key)
        return self._cohort_cache

    def list_cohorts(self,
                     include_child_workspaces: bool = False) -> CohortList:
        """
        Retrieve a list of cohorts for the workspace.

        :param include_child_workspaces: Whether to include cohorts from child workspaces.
        :type include_child_workspaces: bool
        :return: A list of cohorts.
        :rtype: CohortList
        """
        return Cohort.list(include_child_workspaces=include_child_workspaces,
                           api_key=self.api_key)

    @property
    def imports(self) -> List[Import]:
        """Retrieve a cached list of imports for the workspace."""
        if not hasattr(self, '_import_cache'):
            self._import_cache = Import.list(api_key=self.api_key)
        return self._import_cache

    def list_segments(self,
                      import_id: str) -> List[Segment]:
        """
        Retrieve a list of segments for a given import.

        :param import_id: The ID of the import to retrieve segments for.
        :type import_id: str
        :return: A list of segments.
        :rtype: List[Segment]
        """
        return Segment.list(import_id=import_id,
                            api_key=self.api_key)
    @overload
    @classmethod
    def from_json(cls: Type["Workspace"], data: dict) -> "Workspace": ...
    
    @overload
    @classmethod
    def from_json(cls: Type["Workspace"], data: list[dict]) -> list["Workspace"]: ...
    
    @overload
    @classmethod
    def from_json(cls: Type["Workspace"], data: str) -> Union["Workspace", list["Workspace"]]: ...
    @overload
    @classmethod
    def from_json(cls: Type["Workspace"], data: Path) -> Union["Workspace", list["Workspace"]]: ...
    
    @classmethod
    def from_json(cls: Type["Workspace"], data: Any) -> Union["Workspace", list["Workspace"]]:
        """Deserialize workspace data from various JSON representations."""
        return super().from_json(data)


class WorkspaceList(List[Workspace], JSONSerializable):
    """Manage a collection of Workspace objects."""

    def __init__(self,
                 items_list: Optional[List[Workspace]] = None):
        """Initialize the WorkspaceList with an optional list of Workspace objects."""
        super().__init__(items_list if items_list is not None else [])
        self._id_dictionary_cache: Dict[str, Workspace] = {}
        self._name_dictionary_cache: Dict[str, Workspace] = {}
        self.rebuild_cache()

    def rebuild_cache(self):
        """Rebuild all caches based on the current state of the list."""
        self._id_dictionary_cache = {
            workspace.workspace_id: workspace for workspace in self if workspace.workspace_id}
        self._name_dictionary_cache = {
            workspace.name: workspace for workspace in self if workspace.name}

    @property
    def id_dictionary(self) -> Dict[str, Workspace]:
        """Return a dictionary of workspaces indexed by their IDs."""
        if not self._id_dictionary_cache:
            self.rebuild_cache()
        return self._id_dictionary_cache

    @property
    def name_dictionary(self) -> Dict[str, Workspace]:
        """Return a dictionary of workspaces indexed by their names."""
        if not self._name_dictionary_cache:
            self.rebuild_cache()
        return self._name_dictionary_cache

    @property
    def master_workspace(self) -> Workspace:
        """Return the top-level workspace."""
        for workspace in self:
            if workspace.isTopLevel:
                return workspace
        raise ValueError("No Top-Level Workspace found")

    def to_list(self) -> List[Workspace]:
        """Return the list of workspaces."""
        return list(self)
