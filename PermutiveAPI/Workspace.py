from typing import Dict, List, Optional
from collections.abc import Iterable
from dataclasses import dataclass, field
import json
import logging
from typing import List, Optional, Dict
from dataclasses import dataclass
import os


from .Utils import FileHelper, ListHelper
from .Cohort import Cohort, CohortList
from .Query import Query
from .Import import Import
from .Segment import Segment

TAGS = ['#automatic', '#imports']


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
    def isTopLevel(self):
        if self.organizationID == self.workspaceID:
            return True
        return False

    def list_cohorts(self,
                     include_child_workspaces: bool = False) -> CohortList:
        return Cohort.list(include_child_workspaces=include_child_workspaces,
                           privateKey=self.privateKey)

    def list_imports(self) -> List[Import]:
        return Import.list(privateKey=self.privateKey)

    def list_segments(self, import_id: str) -> List[Segment]:
        return Segment.list(import_id=import_id, privateKey=self.privateKey)

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
                            cohorts_list: Optional[CohortList] = None,
                            masterKey: Optional[str] = None):
        import_segments = Segment.list(import_id=import_detail.id,
                                       privateKey=self.privateKey)
        if not import_segments:
            logging.warning("Import has no segment")
            return
        if not cohorts_list:
            cohorts_list = Cohort.list(include_child_workspaces=True,
                                       privateKey=self.privateKey)
        api_key = masterKey if masterKey is not None else self.privateKey
        cohort_tags = ListHelper.merge_list(TAGS, import_detail.name)
        provider_query = Query(name=f"{prefix or ''}{import_detail.name}",
                                    tags=cohort_tags,
                                    second_party_segments=[])

        provider_cohort = cohorts_list.name_dictionary.get(provider_query.name)

        provider_query.id = provider_cohort.id if provider_cohort is not None else None

        if provider_cohort:
            if provider_query.tags:
                provider_query.tags = ListHelper.merge_list(
                    provider_query.tags, provider_cohort.tags)
            else:
                provider_query.tags = provider_cohort.tags
        for import_segment in import_segments:
            logging.debug(
                f"AudienceAPI::sync_cohort::{import_detail.name}::{import_segment.name}")
            t_segment = (import_detail.code, import_segment.code)

            import_segment_query = Query(name=f"{prefix or ''}{import_detail.name} | {import_segment.name}",
                                         description=f'{import_detail.name} ({import_detail.id})::{import_segment.code}::{import_segment.name} ({import_segment.id})',
                                         tags=cohort_tags,
                                         second_party_segments=[t_segment],
                                         workspace_id=self.workspaceID)
            import_segment_cohort = cohorts_list.name_dictionary.get(
                import_segment_query .name)
            import_segment_query.id = import_segment_cohort.id if import_segment_cohort is not None else None

            if import_segment_cohort:
                if import_segment_query.tags:
                    import_segment_query.tags = ListHelper.merge_list(
                        import_segment_query.tags, import_segment_cohort.tags)
                else:
                    import_segment_query.tags = import_segment_cohort.tags
            import_segment_query.sync(api_key=api_key)
            if not provider_query.second_party_segments:
                provider_query.second_party_segments = []
            provider_query.second_party_segments.append(t_segment)
        provider_query.sync(api_key=api_key)

    def sync_imports_segments(self):
        cohorts_list = Cohort.list(include_child_workspaces=True,
                                   privateKey=self.privateKey)
        for item in Import.list(privateKey=self.privateKey):
            self.sync_import_cohorts(import_detail=item,
                                     prefix=f"{self.name} | Import | ",
                                     cohorts_list=cohorts_list)


@dataclass
class WorkspaceList(List[Workspace]):
    # Cache for each dictionary to avoid rebuilding
    _id_dictionary_cache: Dict[str, Workspace] = field(
        default_factory=dict, init=False)
    _name_dictionary_cache: Dict[str, Workspace] = field(
        default_factory=dict, init=False)

    def __init__(self, workspaces: Optional[List[Workspace]] = None):
        """Initializes the WorkspaceList with an optional list of Workspace objects."""
        super().__init__(workspaces if workspaces is not None else [])
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

    def sync_imports_segments(self):
        """Syncs imports and segments for each workspace in the list."""
        for ws in self:
            ws.sync_imports_segments()

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
            json.dump(self, f, ensure_ascii=False, indent=4,
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
