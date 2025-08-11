import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from PermutiveAPI.Audience.Source import Source


class TestSource(unittest.TestCase):
    def setUp(self):
        self.source_data = {
            "id": "source-123",
            "state": {"status": "active"},
            "type": "s3",
        }
        self.source = Source(**self.source_data)

    def test_source_creation(self):
        self.assertEqual(self.source.id, "source-123")
        self.assertEqual(self.source.type, "s3")


if __name__ == "__main__":
    unittest.main()
