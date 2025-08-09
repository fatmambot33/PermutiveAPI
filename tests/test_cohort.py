import sys
from pathlib import Path
import unittest
from unittest.mock import patch, MagicMock
from datetime import timezone

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from PermutiveAPI.Cohort import Cohort, CohortList

class TestCohort(unittest.TestCase):

    def setUp(self):
        self.api_key = "test_api_key"
        self.cohort_data = {
            "id": "cohort-id-123",
            "name": "Test Cohort",
            "query": {"type": "and", "conditions": []},
            "description": "A test cohort",
            "tags": ["tag1", "tag2"],
            "code": "C123"
        }
        self.cohort = Cohort(**self.cohort_data)

    def test_default_datetime_timezone(self):
        cohort = Cohort(name="T", query={"type": "and", "conditions": []})
        self.assertIsNotNone(cohort.created_at)
        self.assertIsNotNone(cohort.last_updated_at)
        self.assertEqual(cohort.created_at.tzinfo, timezone.utc)
        self.assertEqual(cohort.last_updated_at.tzinfo, timezone.utc)

    @patch('PermutiveAPI.Cohort.RequestHelper.post_static')
    def test_create_cohort(self, mock_post):
        # Arrange
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = self.cohort_data
        mock_post.return_value = mock_response

        cohort_to_create = Cohort(name="Test Cohort", query={"type": "and", "conditions": []})

        # Act
        cohort_to_create.create(self.api_key)

        # Assert
        mock_post.assert_called_once()
        self.assertEqual(cohort_to_create.id, self.cohort_data["id"])
        self.assertEqual(cohort_to_create.code, self.cohort_data["code"])

    @patch('PermutiveAPI.Cohort.RequestHelper.patch_static')
    def test_update_cohort(self, mock_patch):
        # Arrange
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = self.cohort_data
        mock_patch.return_value = mock_response

        # Act
        updated_cohort = self.cohort.update(self.api_key)

        # Assert
        mock_patch.assert_called_once()
        self.assertIsInstance(updated_cohort, Cohort)
        self.assertEqual(updated_cohort.id, self.cohort_data["id"])

    @patch('PermutiveAPI.Cohort.RequestHelper.delete_static')
    def test_delete_cohort(self, mock_delete):
        # Arrange
        mock_response = MagicMock()
        mock_response.status_code = 204
        mock_delete.return_value = mock_response

        # Act
        self.cohort.delete(self.api_key)

        # Assert
        mock_delete.assert_called_once_with(api_key=self.api_key, url=f"https://api.permutive.app/cohorts-api/v2/cohorts/{self.cohort.id}")

    @patch('PermutiveAPI.Cohort.RequestHelper.get_static')
    def test_get_by_id(self, mock_get):
        # Arrange
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = self.cohort_data
        mock_get.return_value = mock_response

        # Act
        fetched_cohort = Cohort.get_by_id("cohort-id-123", self.api_key)

        # Assert
        mock_get.assert_called_once()
        self.assertEqual(fetched_cohort.id, self.cohort_data["id"])
        self.assertEqual(fetched_cohort.name, self.cohort_data["name"])

    @patch('PermutiveAPI.Cohort.Cohort.list')
    def test_get_by_name(self, mock_list):
        # Arrange
        mock_list.return_value = CohortList([self.cohort])

        # Act
        found_cohort = Cohort.get_by_name("Test Cohort", self.api_key)

        # Assert
        mock_list.assert_called_once_with(include_child_workspaces=True, api_key=self.api_key)
        self.assertEqual(found_cohort, self.cohort)

    @patch('PermutiveAPI.Cohort.RequestHelper.get_static')
    def test_list_cohorts(self, mock_get):
        # Arrange
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [self.cohort_data, {"id": "cohort-456", "name": "Another Cohort"}]
        mock_get.return_value = mock_response

        # Act
        cohort_list = Cohort.list(self.api_key)

        # Assert
        mock_get.assert_called_once()
        self.assertIsInstance(cohort_list, CohortList)
        self.assertEqual(len(cohort_list), 2)
        self.assertEqual(cohort_list[0].id, "cohort-id-123")


class TestCohortList(unittest.TestCase):

    def setUp(self):
        self.cohorts_data = [
            {"id": "c1", "name": "Cohort 1", "code": "C1", "tags": ["t1"], "workspace_id": "w1", "segment_type": "s1"},
            {"id": "c2", "name": "Cohort 2", "code": "C2", "tags": ["t1", "t2"], "workspace_id": "w1", "segment_type": "s2"},
            {"id": "c3", "name": "Cohort 3", "code": "C3", "tags": ["t2"], "workspace_id": "w2", "segment_type": "s1"}
        ]
        self.cohorts = [Cohort(**data) for data in self.cohorts_data]
        self.cohort_list = CohortList(self.cohorts)

    def test_cohort_list_creation(self):
        self.assertEqual(len(self.cohort_list), 3)

    def test_id_dictionary(self):
        self.assertIn("c1", self.cohort_list.id_dictionary)
        self.assertEqual(self.cohort_list.id_dictionary["c1"].name, "Cohort 1")

    def test_code_dictionary(self):
        self.assertIn("C2", self.cohort_list.code_dictionary)
        self.assertEqual(self.cohort_list.code_dictionary["C2"].name, "Cohort 2")

    def test_name_dictionary(self):
        self.assertIn("Cohort 3", self.cohort_list.name_dictionary)
        self.assertEqual(self.cohort_list.name_dictionary["Cohort 3"].id, "c3")

    def test_tag_dictionary(self):
        self.assertEqual(len(self.cohort_list.tag_dictionary["t1"]), 2)
        self.assertEqual(len(self.cohort_list.tag_dictionary["t2"]), 2)

    def test_workspace_dictionary(self):
        self.assertEqual(len(self.cohort_list.workspace_dictionary["w1"]), 2)
        self.assertEqual(len(self.cohort_list.workspace_dictionary["w2"]), 1)

    def test_segment_type_dictionary(self):
        self.assertEqual(len(self.cohort_list.segment_type_dictionary["s1"]), 2)
        self.assertEqual(len(self.cohort_list.segment_type_dictionary["s2"]), 1)

    def test_rebuild_cache(self):
        """Ensure caches are rebuilt to match the current list state."""
        self.cohort_list.pop(0)
        new_cohort = Cohort(id="c4", name="Cohort 4", code="C4", tags=["t3"], workspace_id="w3", segment_type="s3")
        self.cohort_list.append(new_cohort)
        self.cohort_list.rebuild_cache()

        self.assertNotIn("c1", self.cohort_list.id_dictionary)
        self.assertIn("c4", self.cohort_list.id_dictionary)

        self.assertNotIn("C1", self.cohort_list.code_dictionary)
        self.assertIn("C4", self.cohort_list.code_dictionary)

        self.assertNotIn("Cohort 1", self.cohort_list.name_dictionary)
        self.assertIn("Cohort 4", self.cohort_list.name_dictionary)

        self.assertEqual(len(self.cohort_list.tag_dictionary["t1"]), 1)
        self.assertEqual(len(self.cohort_list.tag_dictionary["t2"]), 2)
        self.assertEqual(len(self.cohort_list.tag_dictionary["t3"]), 1)

        self.assertEqual(len(self.cohort_list.workspace_dictionary["w1"]), 1)
        self.assertEqual(len(self.cohort_list.workspace_dictionary["w2"]), 1)
        self.assertEqual(len(self.cohort_list.workspace_dictionary["w3"]), 1)

        self.assertEqual(len(self.cohort_list.segment_type_dictionary["s1"]), 1)
        self.assertEqual(len(self.cohort_list.segment_type_dictionary["s2"]), 1)
        self.assertEqual(len(self.cohort_list.segment_type_dictionary["s3"]), 1)

if __name__ == '__main__':
    unittest.main()
