from typing import Dict, List, Optional, Any
from dataclasses import dataclass

from PermutiveAPI.Utils import JSONSerializable
from PermutiveAPI.Import import Import, Segment
from PermutiveAPI.Cohort import Cohort, CohortList


@dataclass
class Workspace(JSONSerializable):
    """
    Dataclass for the Workspace entity in the Permutive ecosystem.
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
        if not hasattr(self, '_cohort_cache'):
            self._cohort_cache = Cohort.list(
                include_child_workspaces=False, api_key=self.api_key)
        return self._cohort_cache

    def list_cohorts(self,
                     include_child_workspaces: bool = False) -> CohortList:
        return Cohort.list(include_child_workspaces=include_child_workspaces,
                           api_key=self.api_key)

    @property
    def imports(self) -> List[Import]:
        if not hasattr(self, '_import_cache'):
            self._import_cache = Import.list(api_key=self.api_key)
        return self._import_cache

    def list_segments(self,
                      import_id: str) -> List[Segment]:
        return Segment.list(import_id=import_id,
                            api_key=self.api_key)


class WorkspaceList(List[Workspace], JSONSerializable):
    def __init__(self,
                 items_list: Optional[List[Workspace]] = None):
        """Initializes the WorkspaceList with an optional list of Workspace objects."""
        super().__init__(items_list if items_list is not None else [])
        self._id_dictionary_cache: Dict[str, Workspace] = {}
        self._name_dictionary_cache: Dict[str, Workspace] = {}
        self.rebuild_cache()

    def rebuild_cache(self):
        """Rebuilds all caches based on the current state of the list."""
        self._id_dictionary_cache = {
            workspace.workspace_id: workspace for workspace in self if workspace.workspace_id}
        self._name_dictionary_cache = {
            workspace.name: workspace for workspace in self if workspace.name}

    @property
    def id_dictionary(self) -> Dict[str, Workspace]:
        """Returns a dictionary of workspaces indexed by their IDs."""
        if not self._id_dictionary_cache:
            self.rebuild_cache()
        return self._id_dictionary_cache

    @property
    def name_dictionary(self) -> Dict[str, Workspace]:
        """Returns a dictionary of workspaces indexed by their names."""
        if not self._name_dictionary_cache:
            self.rebuild_cache()
        return self._name_dictionary_cache

    @property
    def Masterworkspace(self) -> Workspace:
        """Returns the top-level workspace."""
        for workspace in self:
            if workspace.isTopLevel:
                return workspace
        raise ValueError("No Top-Level Workspace found")

    def to_list(self) -> List[Workspace]:
        """Returns the list of workspaces."""
        return list(self)
