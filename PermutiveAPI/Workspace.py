
from dataclasses import dataclass
from typing import List, Optional
import os
from .Utils import FileHelper
from .APIRequestHandler import Cohort,Import, Segment



@dataclass
class Workspace(FileHelper):
    """
    Dataclass for the Workspace entity in the Permutive ecosystem.
    """
    name: str
    organizationID: str
    workspaceID: str
    privateKey: str

    @property
    def isTopLevel(self):
        if self.organizationID == self.workspaceID:
            return True
        return False

    def list_cohorts(self,
                     include_child_workspaces:bool=False):
        return Cohort.list(api_key=self.privateKey,
                           include_child_workspaces=include_child_workspaces)
    def create_cohort(self,
                      cohort:Cohort)->Cohort:
        return Cohort.create(api_key=self.privateKey,
                             cohort=cohort)
    def update_cohort(self,
                      cohort:Cohort)->Cohort:
        return Cohort.update(api_key=self.privateKey,
                             cohort=cohort)
    def delete_cohort(self,
                      cohort:Cohort):
        Cohort.delete(api_key=self.privateKey,
                             cohort=cohort)
    
    def list_imports(self):
        return Import.list(api_key=self.privateKey)
    
    def get_import(self,import_id:str) ->Import:
        return Import.get(api_key=self.privateKey,
                          import_id=import_id)

    
    def list_segments(self,import_id:str):
        return Segment.list(api_key=self.privateKey,import_id=import_id)
    
    def create_segment(self,
                      segment:Segment)->Segment:
        return Segment.create(api_key=self.privateKey,
                             segment=segment)
    def update_segment(self,
                      segment:Segment)->Segment:
        return Segment.update(api_key=self.privateKey,
                             segment=segment)
    def delete_segment(self,
                      segment:Segment):
        Segment.delete(api_key=self.privateKey,
                             segment=segment)

@dataclass
class WorkspaceList(List[Workspace]):


    def __init__(self, workspaces: Optional[List[Workspace]] = None):
        super().__init__(workspaces if workspaces is not None else [])
        self._id_map = {workspace.workspaceID: workspace for workspace in self}
        self._name_map = {workspace.name: workspace for workspace in self}

    def get_by_id(self, workspaceID: str) -> Optional[Workspace]:
        return self._id_map.get(workspaceID, None)

    def get_by_name(self, name: str) -> Optional[Workspace]:
        return self._name_map.get(name, None)
    @property
    def Masterworkspace(self) -> Workspace:
        for workspace in self:
            if workspace.isTopLevel:
                return workspace
        raise ValueError("No Top WS")

    def to_json(self, filepath: str):
        FileHelper.to_json(self, filepath=filepath)

    @staticmethod
    def from_json(filepath: Optional[str] = None) -> 'WorkspaceList':
        if filepath is None:
            filepath = os.environ.get("PERMUTIVE_APPLICATION_CREDENTIALS")
        if filepath is None:
            raise ValueError(
                'Unable to get PERMUTIVE_APPLICATION_CREDENTIALS from .env')

        workspace_list = FileHelper.from_json(filepath)
        if not isinstance(workspace_list, list):
            raise TypeError("Expected a list of workspaces from the JSON file")

        return WorkspaceList([Workspace(**workspace) for workspace in workspace_list])
