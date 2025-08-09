import sys
from pathlib import Path
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from PermutiveAPI.Workspace import Workspace, WorkspaceList
from PermutiveAPI.Cohort import Cohort, CohortList
from PermutiveAPI.Import import Import
from PermutiveAPI.Segment import Segment

class TestWorkspace(unittest.TestCase):

    def setUp(self):
        self.api_key = "test_api_key"
        self.workspace_data = {
            "name": "Test Workspace",
            "organisation_id": "org-123",
            "workspace_id": "ws-123",
            "api_key": self.api_key
        }
        self.workspace = Workspace(**self.workspace_data)

    def test_isTopLevel(self):
        # Arrange
        top_level_workspace = Workspace(name="Top Level", organisation_id="org-123", workspace_id="org-123", api_key=self.api_key)

        # Act & Assert
        self.assertTrue(top_level_workspace.isTopLevel)
        self.assertFalse(self.workspace.isTopLevel)

    @patch('PermutiveAPI.Workspace.Cohort.list')
    def test_cohorts_property(self, mock_cohort_list):
        # Arrange
        mock_cohort_list.return_value = CohortList()

        # Act
        cohorts = self.workspace.cohorts

        # Assert
        mock_cohort_list.assert_called_once_with(include_child_workspaces=False, api_key=self.api_key)
        self.assertIsInstance(cohorts, CohortList)
        # test caching
        self.workspace.cohorts
        mock_cohort_list.assert_called_once()


    @patch('PermutiveAPI.Workspace.Cohort.list')
    def test_list_cohorts(self, mock_cohort_list):
        # Arrange
        mock_cohort_list.return_value = CohortList()

        # Act
        self.workspace.list_cohorts(include_child_workspaces=True)

        # Assert
        mock_cohort_list.assert_called_with(include_child_workspaces=True, api_key=self.api_key)

    @patch('PermutiveAPI.Workspace.Import.list')
    def test_imports_property(self, mock_import_list):
        # Arrange
        mock_import_list.return_value = []

        # Act
        imports = self.workspace.imports

        # Assert
        mock_import_list.assert_called_once_with(api_key=self.api_key)
        self.assertIsInstance(imports, list)
        # test caching
        self.workspace.imports
        mock_import_list.assert_called_once()

    @patch('PermutiveAPI.Workspace.Segment.list')
    def test_list_segments(self, mock_segment_list):
        # Arrange
        mock_segment_list.return_value = []

        # Act
        self.workspace.list_segments(import_id="import-123")

        # Assert
        mock_segment_list.assert_called_with(import_id="import-123", api_key=self.api_key)


class TestWorkspaceList(unittest.TestCase):
    def setUp(self):
        self.workspaces_data = [
            {"name": "Master Workspace", "organisation_id": "org-123", "workspace_id": "org-123", "api_key": "key1"},
            {"name": "Child Workspace 1", "organisation_id": "org-123", "workspace_id": "ws-1", "api_key": "key2"},
            {"name": "Child Workspace 2", "organisation_id": "org-123", "workspace_id": "ws-2", "api_key": "key3"},
        ]
        self.workspaces = [Workspace(**data) for data in self.workspaces_data]
        self.workspace_list = WorkspaceList(self.workspaces)

    def test_rebuild_cache(self):
        # Act
        self.workspace_list.rebuild_cache()

        # Assert
        self.assertEqual(len(self.workspace_list.id_dictionary), 3)
        self.assertEqual(len(self.workspace_list.name_dictionary), 3)
        self.assertEqual(self.workspace_list.id_dictionary["ws-1"].name, "Child Workspace 1")
        self.assertEqual(self.workspace_list.name_dictionary["Child Workspace 2"].workspace_id, "ws-2")

    def test_id_dictionary(self):
        self.assertIn("ws-1", self.workspace_list.id_dictionary)
        self.assertEqual(self.workspace_list.id_dictionary["ws-1"].name, "Child Workspace 1")

    def test_name_dictionary(self):
        self.assertIn("Child Workspace 2", self.workspace_list.name_dictionary)
        self.assertEqual(self.workspace_list.name_dictionary["Child Workspace 2"].workspace_id, "ws-2")

    def test_master_workspace(self):
        master = self.workspace_list.master_workspace
        self.assertEqual(master.name, "Master Workspace")

    def test_master_workspace_not_found(self):
        # Arrange
        child_workspaces = [ws for ws in self.workspaces if not ws.isTopLevel]
        workspace_list = WorkspaceList(child_workspaces)

        # Act & Assert
        with self.assertRaises(ValueError):
            workspace_list.master_workspace

if __name__ == '__main__':
    unittest.main()
