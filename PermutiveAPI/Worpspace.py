import logging
import os
from typing import Dict, List, Optional
from dataclasses import dataclass, field,asdict
import json


from .Utils import FileHelper
from .Import import Import, Segment
from .Cohort import Cohort, CohortList

TAGS = ['#automatic']


@dataclass
class Workspace():
    """
    Dataclass for the Workspace entity in the Permutive ecosystem.
    """
    name: str
    organizationID: str
    workspaceID: str
    privateKey: str

    @property
    def isTopLevel(self) -> bool:
        """Determines if the workspace is the top-level workspace."""
        return self.organizationID == self.workspaceID

    def list_cohorts(self,
                     include_child_workspaces: bool = False) -> CohortList:
        return Cohort.list(include_child_workspaces=include_child_workspaces,
                           privateKey=self.privateKey)

    def list_imports(self) -> List[Import]:
        return Import.list(privateKey=self.privateKey)

    def list_segments(self, import_id: str) -> List[Segment]:
        return Segment.list(import_id=import_id, privateKey=self.privateKey)


class WorkspaceList(List[Workspace]):
    def __init__(self, workspaces: Optional[List[Workspace]] = None):
        """Initializes the WorkspaceList with an optional list of Workspace objects."""
        super().__init__(workspaces if workspaces is not None else [])
        self._id_dictionary_cache: Dict[str, Workspace] = {}
        self._name_dictionary_cache: Dict[str, Workspace] = {}
        self.rebuild_cache()

    def rebuild_cache(self):
        """Rebuilds all caches based on the current state of the list."""
        self._id_dictionary_cache = {
            workspace.workspaceID: workspace for workspace in self if workspace.workspaceID}
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

    def to_json(self, filepath: str):
        """Saves the WorkspaceList to a JSON file at the specified filepath."""
        FileHelper.check_filepath(filepath)
        with open(file=filepath, mode='w', encoding='utf-8') as f:
            json.dump([asdict(ws) for ws in self], f, ensure_ascii=False, indent=4,
                      default=FileHelper.json_default)

    @staticmethod
    def from_json(filepath: Optional[str] = None) -> 'WorkspaceList':
        """Creates a new WorkspaceList from a JSON file at the specified filepath."""
        if not filepath:
            filepath = os.environ.get("PERMUTIVE_APPLICATION_CREDENTIALS")
        if not filepath:
            raise ValueError(
                'Unable to get PERMUTIVE_APPLICATION_CREDENTIALS from .env')

        workspace_list = FileHelper.from_json(filepath)
        return WorkspaceList([Workspace(**workspace) for workspace in workspace_list])
