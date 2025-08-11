import sys
from pathlib import Path
import unittest
from unittest.mock import patch, MagicMock
from datetime import timezone

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from PermutiveAPI.Audience.Segment import Segment, SegmentList


class TestSegment(unittest.TestCase):
    def setUp(self):
        self.api_key = "test_api_key"
        self.segment_data = {
            "id": "seg-123",
            "import_id": "import-123",
            "name": "Test Segment",
            "code": "S123",
            "cpm": 0.0,
        }
        self.segment = Segment(**self.segment_data)

    @patch("PermutiveAPI.Audience.Segment.RequestHelper.post_static")
    def test_create(self, mock_post):
        # Arrange
        mock_response = MagicMock()
        mock_response.json.return_value = self.segment_data
        mock_post.return_value = mock_response
        segment_to_create = Segment(
            import_id="import-123", name="Test Segment", code="S123"
        )

        # Act
        segment_to_create.create(self.api_key)

        # Assert
        mock_post.assert_called_once()
        self.assertEqual(segment_to_create.id, "seg-123")

    @patch("PermutiveAPI.Audience.Segment.RequestHelper.patch_static")
    def test_update(self, mock_patch):
        # Arrange
        mock_response = MagicMock()
        mock_response.json.return_value = self.segment_data
        mock_patch.return_value = mock_response

        # Act
        self.segment.update(self.api_key)

        # Assert
        mock_patch.assert_called_once()

    @patch("PermutiveAPI.Audience.Segment.RequestHelper.delete_static")
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

    @patch("PermutiveAPI.Audience.Segment.RequestHelper.delete_static")
    def test_delete_logs_import_id(self, mock_delete):
        # Arrange
        mock_response = MagicMock()
        mock_response.status_code = 204
        mock_delete.return_value = mock_response

        with patch("PermutiveAPI.Audience.Segment.logging.debug") as mock_log:
            # Act
            self.segment.delete(self.api_key)

            # Assert
            mock_log.assert_called_with(
                "SegmentAPI::delete_segment::import-123::seg-123"
            )

    @patch("PermutiveAPI.Audience.Segment.RequestHelper.get_static")
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

    @patch("PermutiveAPI.Audience.Segment.RequestHelper.get_static")
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

    @patch("PermutiveAPI.Audience.Segment.RequestHelper.get_static")
    def test_list(self, mock_get):
        # Arrange
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "elements": [self.segment_data],
            "pagination": {},
        }
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
        self.assertEqual(seg.updated_at.tzinfo, timezone.utc)  # type: ignore


class TestSegmentList(unittest.TestCase):
    def setUp(self):
        self.segments_data = [
            {
                "id": "seg1",
                "name": "Segment 1",
                "code": "S1",
                "import_id": "imp1",
                "cpm": 0.0,
            },
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


if __name__ == "__main__":
    unittest.main()
