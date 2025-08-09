"""Tests for helper utilities in :mod:`PermutiveAPI.Utils`."""

import sys
from pathlib import Path
import unittest
from tempfile import TemporaryDirectory
from unittest.mock import patch

from requests.exceptions import RequestException

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from PermutiveAPI.Utils import RequestHelper, ListHelper, FileHelper


class TestRequestHelper(unittest.TestCase):
    """Tests for :class:`RequestHelper`."""

    def test_generate_url_with_key_no_query(self) -> None:
        """Append API key with ``?`` when the URL lacks query parameters."""
        url = "https://api.example.com/resource"
        result = RequestHelper.generate_url_with_key(url, "abc123")
        self.assertEqual(result, "https://api.example.com/resource?k=abc123")

    def test_generate_url_with_key_with_query(self) -> None:
        """Append API key with ``&`` when the URL already has a query."""
        url = "https://api.example.com/resource?x=1"
        result = RequestHelper.generate_url_with_key(url, "abc123")
        self.assertEqual(result, "https://api.example.com/resource?x=1&k=abc123")

    def test_retry_respects_max_retries(self) -> None:
        """Stop after the configured number of retries."""
        with (
            patch.object(RequestHelper, "MAX_RETRIES", 2),
            patch("time.sleep", return_value=None),
            patch("requests.get", side_effect=RequestException) as mock_get,
        ):
            with self.assertRaises(RequestException):
                RequestHelper.get_static("abc123", "https://api.example.com")
            self.assertEqual(mock_get.call_count, 2)


class TestListHelper(unittest.TestCase):
    """Tests for list utility helpers."""

    def test_merge_list_with_duplicates_and_sort(self) -> None:
        """Merge lists, remove duplicates and sort the result."""
        merged = ListHelper.merge_list([3, 1, 2], [2, 4])
        self.assertEqual(merged, [1, 2, 3, 4])

    def test_merge_list_with_single_value(self) -> None:
        """Handle merging when the second argument is a scalar."""
        merged = ListHelper.merge_list(["b", "a"], "c")
        self.assertEqual(merged, ["a", "b", "c"])


class TestFileHelper(unittest.TestCase):
    """Tests for file helper utilities."""

    def test_split_filepath(self) -> None:
        """Split filepath into directory, base name and extension."""
        path = "/tmp/data/report.tar.gz"
        file_path, file_name, file_ext = FileHelper.split_filepath(path)
        self.assertEqual(file_path, "/tmp/data/")
        self.assertEqual(file_name, "report")
        self.assertEqual(file_ext, ".tar.gz")

    def test_file_exists_with_pattern(self) -> None:
        """Detect existing files matching the pattern ``<name>-*<ext>``."""
        with TemporaryDirectory() as tmpdir:
            file_with_suffix = Path(tmpdir) / "report-123.txt"
            file_with_suffix.write_text("data", encoding="utf-8")
            self.assertTrue(FileHelper.file_exists(str(Path(tmpdir) / "report.txt")))
            self.assertFalse(FileHelper.file_exists(str(Path(tmpdir) / "missing.txt")))


if __name__ == "__main__":  # pragma: no cover - manual execution
    unittest.main()

