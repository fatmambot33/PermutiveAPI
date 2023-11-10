import json
import logging
from typing import List, Optional
from dataclasses import dataclass
import os


from .Utils import FileHelper, ListHelper
from .Cohort import Cohort
from .Query import Query
from .Import import Import
from .Segment import Segment


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
                     include_child_workspaces: bool = False) -> List[Cohort]:
        return Cohort.list(include_child_workspaces=include_child_workspaces,
                           privateKey=self.privateKey)

    def list_imports(self) -> List[Import]:
        return Import.list(privateKey=self.privateKey)

    def list_segments(self, import_id: str) -> List[Segment]:
        return Segment.list(id=import_id, privateKey=self.privateKey)

    def sync_imports_cohorts(self,
                             import_detail: 'Import',
                             prefix: Optional[str] = None,
                             inheritance: bool = False,
                             masterKey: Optional[str] = None):
        cohorts_list = self.list_cohorts(include_child_workspaces=True)
        for import_detail in Import.list(privateKey=self.privateKey):
            if (inheritance and import_detail.inheritance) or (not inheritance and not import_detail.inheritance):
                self.sync_import_cohorts(import_detail=import_detail,
                                         prefix=prefix,
                                         cohorts_list=cohorts_list,
                                         masterKey=masterKey)

    def sync_import_cohorts(self,
                            import_detail: 'Import',
                            prefix: Optional[str] = None,
                            cohorts_list: Optional[List['Cohort']] = None,
                            masterKey: Optional[str] = None):
        import_segments = Segment.list(id=import_detail.id,
                                       privateKey=self.privateKey)
        if len(import_segments) == 0:
            return
        if not cohorts_list:
            cohorts_list = Cohort.list(include_child_workspaces=True,
                                       privateKey=self.privateKey)
        api_key = masterKey if masterKey is not None else self.privateKey
        q_provider_segments = Query(name=f"{prefix or ''}{import_detail.name}",
                                    tags=[import_detail.name,
                                          'automatic', 'imports'],
                                    second_party_segments=[])
        q_provider_segments.id = next(
            (cohort.id for cohort in cohorts_list if cohort.name == q_provider_segments.name), None)
        cohort_tags = next(
            (cohort.tags for cohort in cohorts_list if cohort.name == q_provider_segments.name), None)
        if q_provider_segments.tags:
            q_provider_segments.tags = ListHelper.merge_list(
                q_provider_segments.tags, cohort_tags)
        else:
            q_provider_segments.tags = cohort_tags
        for import_segment in import_segments:
            logging.debug(
                f"AudienceAPI::sync_cohort::{import_detail.name}::{import_segment.name}")
            t_segment = (import_detail.code, import_segment.code)

            q_segment = Query(name=f"{prefix or ''}{import_detail.name} | {import_segment.name}",
                              description=f'{import_detail.name} ({import_detail.id}) : {import_segment.code} : {import_segment.name} ({import_segment.id})',
                              tags=[import_detail.name,
                                    '#automatic', '#imports'],
                              second_party_segments=[t_segment],
                              workspace_id=self.workspaceID)
            q_segment.id = next(
                (cohort.id for cohort in cohorts_list if cohort.name == q_segment.name), None)

            if q_segment.id:
                cohort_tags = next(
                    (cohort.tags for cohort in cohorts_list if cohort.id == q_segment.id), None)
            if q_segment.tags:
                q_segment.tags = ListHelper.merge_list(
                    q_segment.tags, cohort_tags)
            else:
                q_segment.tags = cohort_tags
            q_segment.sync(api_key=api_key)
            if not q_provider_segments.second_party_segments:
                q_provider_segments.second_party_segments = []
            q_provider_segments.second_party_segments.append(t_segment)
        q_provider_segments.sync(api_key=api_key)

    def sync_imports_segments(self):
        cohorts_list = Cohort.list(include_child_workspaces=True,
                                   privateKey=self.privateKey)
        for item in Import.list(privateKey=self.privateKey):
            self.sync_import_cohorts(import_detail=item,
                                     prefix=f"{self.name} | Import | ",
                                     cohorts_list=cohorts_list)


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

    def sync_imports_segments(self):
        for ws in self:
            ws.sync_imports_segments()

    @property
    def Masterworkspace(self) -> Workspace:
        for workspace in self:
            if workspace.isTopLevel:
                return workspace
        raise ValueError("No Top WS")

    def from_description(self):
        cohorts_list = Cohort.list(include_child_workspaces=True,
                                   privateKey=self.Masterworkspace.privateKey)
        for cohort in cohorts_list:
            if cohort.tags:
                if "from_description" in cohort.tags and cohort.description:
                    keywords = cohort.description.split(",")
                    query = Query(name=cohort.name, id=cohort.id,
                                  keywords=keywords)
                    query.sync(self.Masterworkspace.privateKey)

    def to_json(self, filepath: str):
        FileHelper.check_filepath(filepath)
        with open(file=filepath, mode='w', encoding='utf-8') as f:
            json.dump(self, f,
                      ensure_ascii=False, indent=4, default=FileHelper.json_default)


    @staticmethod
    def from_json(filepath: Optional[str] = None) -> 'WorkspaceList':
        if filepath is None:
            filepath = os.environ.get("PERMUTIVE_APPLICATION_CREDENTIALS")
        if filepath is None:
            raise ValueError(
                'Unable to get PERMUTIVE_APPLICATION_CREDENTIALS from .env')

        workspace_list = FileHelper.from_json(filepath)
        return WorkspaceList([Workspace(**workspace) for workspace in workspace_list])

