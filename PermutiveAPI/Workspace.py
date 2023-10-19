from dataclasses import dataclass
from typing import List, Optional
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
        FileHelper.to_json(self, filepath=filepath)

    @staticmethod
    def from_file(filepath: str):
        jsonObj = FileHelper.from_json(filepath=filepath)
        return Workspace(**jsonObj)


@dataclass
class WorkspaceList(List[Workspace]):

    def __init__(self, workspaces: Optional[List[Workspace]]):
        if workspaces is not None:
            super().__init__(workspaces)

    def get_privateKey(self, workspaceID: str) -> str:
        for workspace in self:
            if workspace.workspaceID == workspaceID:
                return workspace.privateKey
        raise ValueError("workspaceID does not exist")

    def get_MasterprivateKey(self) -> str:

        return self.get_Masterworkspace().privateKey

    def get_Masterworkspace(self) -> Workspace:
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
