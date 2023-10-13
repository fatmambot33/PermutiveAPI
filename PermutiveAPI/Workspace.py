from dataclasses import dataclass
from typing import List, Optional
from glob import glob
import os
from .Utils import FileHelper


@dataclass
class Workspace:
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

    def to_file(self, filepath: str):
        FileHelper.save_to_json(self, filepath=filepath)

    @staticmethod
    def from_file(filepath: str):
        jsonObj = FileHelper.read_json(filepath=filepath)
        return Workspace(**jsonObj)


@dataclass
class WorkspaceList(List[Workspace]):

    def __init__(self, workspaces: Optional[List[Workspace]]):
        if workspaces is not None:
            super().__init__(workspaces)

    def get_privateKey(self, workspaceID: str) -> Optional[str]:
        for workspace in self:
            if workspace.workspaceID == workspaceID:
                return workspace.privateKey
        else:
            return None

    def get_MasterprivateKey(self) -> Optional[str]:
        for workspace in self:
            if workspace.isTopLevel:
                return workspace.privateKey
        else:
            return None

    @staticmethod
    def read_json(filepath: Optional[str] = "") -> 'WorkspaceList':
        folder_name = "workspace"
        workspace_list = []
        files = List[str]
        if filepath != "":
            files = [filepath]
        else:
            files = glob(os.environ.get("DATA_PATH") + folder_name + '/*.json')
        files.sort()  # type: ignore
        for file_path in files:
            if file_path is not None and FileHelper.file_exists(file_path):
                definitions = FileHelper.read_json(file_path)
                if not isinstance(definitions, List):
                    definitions = [definitions]
                for definition in definitions:
                    workspace_list.append(Workspace(**definition))
        return WorkspaceList(workspace_list)
