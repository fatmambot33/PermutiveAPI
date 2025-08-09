import sys
from pathlib import Path
import unittest
from unittest.mock import patch, MagicMock
from datetime import timezone

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from PermutiveAPI.Source import Source
from PermutiveAPI.Import import Import, ImportList
from PermutiveAPI.Segment import Segment, SegmentList

class TestSource(unittest.TestCase):
    def setUp(self):
        self.source_data = {
            "id": "source-123",
            "state": {"status": "active"},
            "type": "s3"
        }
        self.source = Source(**self.source_data)

    def test_source_creation(self):
        self.assertEqual(self.source.id, "source-123")
        self.assertEqual(self.source.type, "s3")

class TestImport(unittest.TestCase):
    def setUp(self):
        self.api_key = "test_api_key"
        self.source_data = {"id": "src-1", "state": {}, "type": "type"}
        self.import_data = {
            "id": "import-123",
            "name": "Test Import",
            "code": "I123",
            "relation": "some-relation",
            "identifiers": ["id1", "id2"],
            "source": self.source_data,
            "updated_at": "2023-01-01T00:00:00Z"
        }

    @patch('PermutiveAPI.Import.RequestHelper.get_static')
    def test_get_by_id(self, mock_get):
        # Arrange
        mock_response = MagicMock()
        mock_response.json.return_value = self.import_data
        mock_get.return_value = mock_response

        # Act
        result = Import.get_by_id("import-123", self.api_key)

        # Assert
        mock_get.assert_called_once_with(url="https://api.permutive.app/audience-api/v1/imports/import-123", api_key=self.api_key)
        self.assertIsInstance(result, Import)
        self.assertEqual(result.id, "import-123")

    @patch('PermutiveAPI.Import.RequestHelper.get_static')
    def test_list(self, mock_get):
        # Arrange
        mock_response = MagicMock()
        mock_response.json.return_value = {"items": [self.import_data]}
        mock_get.return_value = mock_response

        # Act
        results = Import.list(self.api_key)

        # Assert
        mock_get.assert_called_once_with(api_key=self.api_key, url="https://api.permutive.app/audience-api/v1/imports")
        self.assertIsInstance(results, list)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].id, "import-123")

    def test_default_updated_at_timezone(self):
        source = Source(**self.source_data)
        imp = Import(id="imp", name="N", code="C", relation="r", identifiers=[], source=source)
        self.assertIsNotNone(imp.updated_at)
        self.assertEqual(imp.updated_at.tzinfo, timezone.utc)

class TestImportList(unittest.TestCase):
    def setUp(self):
        source = Source(id="src-1", state={}, type="type")
        self.imports_data = [
            {"id": "imp1", "name": "Import 1", "code": "I1", "relation": "r1", "identifiers": ["id1"], "source": source},
            {"id": "imp2", "name": "Import 2", "code": "I2", "relation": "r2", "identifiers": ["id1", "id2"], "source": source},
            {"id": "imp3", "name": "Import 3", "code": "I3", "relation": "r3", "identifiers": ["id2"], "source": source}
        ]
        self.imports = [Import(**data) for data in self.imports_data]
        self.import_list = ImportList(self.imports)

    def test_import_list_creation(self):
        self.assertEqual(len(self.import_list), 3)

    def test_id_dictionary(self):
        self.assertIn("imp1", self.import_list.id_dictionary)
        self.assertEqual(self.import_list.id_dictionary["imp1"].name, "Import 1")

    def test_name_dictionary(self):
        self.assertIn("Import 2", self.import_list.name_dictionary)
        self.assertEqual(self.import_list.name_dictionary["Import 2"].id, "imp2")

    def test_identifier_dictionary(self):
        self.assertEqual(len(self.import_list.identifier_dictionary["id1"]), 2)
        self.assertEqual(len(self.import_list.identifier_dictionary["id2"]), 2)

    def test_rebuild_cache_after_mutation(self):
        new_import = Import(id="imp4", name="Import 4", code="I4", relation="r4", identifiers=["id2"], source=Source(id="src-1", state={}, type="type"))
        self.import_list.append(new_import)
        self.import_list.pop(0)
        self.import_list.rebuild_cache()
        self.assertNotIn("imp1", self.import_list.id_dictionary)
        self.assertIn("imp4", self.import_list.id_dictionary)
        self.assertNotIn("Import 1", self.import_list.name_dictionary)
        self.assertIn("Import 4", self.import_list.name_dictionary)
        self.assertEqual(len(self.import_list.identifier_dictionary["id1"]), 1)
        self.assertEqual(self.import_list.identifier_dictionary["id1"][0].id, "imp2")
        id2_ids = [imp.id for imp in self.import_list.identifier_dictionary["id2"]]
        self.assertEqual(len(id2_ids), len(set(id2_ids)))
        self.assertCountEqual(id2_ids, ["imp2", "imp3", "imp4"])

class TestSegment(unittest.TestCase):
    def setUp(self):
        self.api_key = "test_api_key"
        self.segment_data = {
            "id": "seg-123",
            "import_id": "import-123",
            "name": "Test Segment",
            "code": "S123",
        }
        self.segment = Segment(**self.segment_data)

    @patch('PermutiveAPI.Segment.RequestHelper.post_static')
    def test_create(self, mock_post):
        # Arrange
        mock_response = MagicMock()
        mock_response.json.return_value = self.segment_data
        mock_post.return_value = mock_response
        segment_to_create = Segment(import_id="import-123", name="Test Segment", code="S123")

        # Act
        segment_to_create.create(self.api_key)

        # Assert
        mock_post.assert_called_once()
        self.assertEqual(segment_to_create.id, "seg-123")

    @patch('PermutiveAPI.Segment.RequestHelper.patch_static')
    def test_update(self, mock_patch):
        # Arrange
        mock_response = MagicMock()
        mock_response.json.return_value = self.segment_data
        mock_patch.return_value = mock_response

        # Act
        self.segment.update(self.api_key)

        # Assert
        mock_patch.assert_called_once()

    @patch('PermutiveAPI.Segment.RequestHelper.delete_static')
    def test_delete(self, mock_delete):
        # Arrange
        mock_response = MagicMock()
        mock_response.status_code = 204
        mock_delete.return_value = mock_response

        # Act
        result = self.segment.delete(self.api_key)

        # Assert
        mock_delete.assert_called_once()
        self.assertTrue(result)

    @patch('PermutiveAPI.Segment.RequestHelper.delete_static')
    def test_delete_logs_import_id(self, mock_delete):
        # Arrange
        mock_response = MagicMock()
        mock_response.status_code = 204
        mock_delete.return_value = mock_response

        with patch('PermutiveAPI.Segment.logging.debug') as mock_log:
            # Act
            self.segment.delete(self.api_key)

            # Assert
            mock_log.assert_called_with(
                'SegmentAPI::delete_segment::import-123::seg-123')

    @patch('PermutiveAPI.Segment.RequestHelper.get_static')
    def test_get_by_code(self, mock_get):
        # Arrange
        mock_response = MagicMock()
        mock_response.json.return_value = self.segment_data
        mock_get.return_value = mock_response

        # Act
        result = Segment.get_by_code("import-123", "S123", self.api_key)

        # Assert
        mock_get.assert_called_once()
        self.assertEqual(result.code, "S123")

    @patch('PermutiveAPI.Segment.RequestHelper.get_static')
    def test_get_by_id(self, mock_get):
        # Arrange
        mock_response = MagicMock()
        mock_response.json.return_value = self.segment_data
        mock_get.return_value = mock_response

        # Act
        result = Segment.get_by_id("import-123", "seg-123", self.api_key)

        # Assert
        mock_get.assert_called_once()
        self.assertEqual(result.id, "seg-123")

    @patch('PermutiveAPI.Segment.RequestHelper.get_static')
    def test_list(self, mock_get):
        # Arrange
        mock_response = MagicMock()
        mock_response.json.return_value = {"elements": [self.segment_data], "pagination": {}}
        mock_get.return_value = mock_response

        # Act
        results = Segment.list("import-123", self.api_key)

        # Assert
        mock_get.assert_called_once()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].id, "seg-123")

    def test_default_updated_at_timezone(self):
        seg = Segment(import_id="import-123", name="Test Segment", code="S123")
        self.assertIsNotNone(seg.updated_at)
        self.assertEqual(seg.updated_at.tzinfo, timezone.utc)

class TestSegmentList(unittest.TestCase):
    def setUp(self):
        self.segments_data = [
            {"id": "seg1", "name": "Segment 1", "code": "S1", "import_id": "imp1"},
            {"id": "seg2", "name": "Segment 2", "code": "S2", "import_id": "imp1"},
            {"id": "seg3", "name": "Segment 3", "code": "S3", "import_id": "imp2"},
        ]
        self.segments = [Segment(**data) for data in self.segments_data]
        self.segment_list = SegmentList(self.segments)

    def test_segment_list_creation(self):
        self.assertEqual(len(self.segment_list), 3)

    def test_id_dictionary(self):
        self.assertIn("seg1", self.segment_list.id_dictionary)
        self.assertEqual(self.segment_list.id_dictionary["seg1"].name, "Segment 1")

    def test_name_dictionary(self):
        self.assertIn("Segment 2", self.segment_list.name_dictionary)
        self.assertEqual(self.segment_list.name_dictionary["Segment 2"].id, "seg2")

    def test_code_dictionary(self):
        self.assertIn("S3", self.segment_list.code_dictionary)
        self.assertEqual(self.segment_list.code_dictionary["S3"].name, "Segment 3")


if __name__ == '__main__':
    unittest.main()
