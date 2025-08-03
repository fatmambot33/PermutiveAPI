import sys
from pathlib import Path
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from PermutiveAPI.User import Alias, Identity


class TestAlias(unittest.TestCase):
    def test_alias_creation(self):
        """Test that an Alias object is created with the correct attributes."""
        alias = Alias(id="test_id", tag="test_tag", priority=1)
        self.assertEqual(alias.id, "test_id")
        self.assertEqual(alias.tag, "test_tag")
        self.assertEqual(alias.priority, 1)

    def test_alias_to_json(self):
        """Test that the to_json method returns the correct dictionary."""
        alias = Alias(id="test_id", tag="test_tag", priority=1)
        self.assertEqual(alias.to_json(), {"id": "test_id", "tag": "test_tag", "priority": 1})


class TestIdentity(unittest.TestCase):
    def setUp(self):
        """Set up a common Identity object for tests."""
        self.alias1 = Alias(id="alias1", tag="tag1", priority=1)
        self.alias2 = Alias(id="alias2", tag="tag2", priority=2)
        self.identity = Identity(user_id="test_user", aliases=[self.alias1, self.alias2])

    def test_identity_creation(self):
        """Test that an Identity object is created with the correct attributes."""
        self.assertEqual(self.identity.user_id, "test_user")
        self.assertEqual(len(self.identity.aliases), 2)
        self.assertEqual(self.identity.aliases[0].id, "alias1")

    def test_identity_to_json(self):
        """Test that the to_json method returns the correct dictionary."""
        expected_dict = {
            "user_id": "test_user",
            "aliases": [
                {"id": "alias1", "tag": "tag1", "priority": 1},
                {"id": "alias2", "tag": "tag2", "priority": 2},
            ],
        }
        self.assertEqual(self.identity.to_json(), expected_dict)

    @patch('PermutiveAPI.User.RequestHelper.post_static')
    def test_identify_call(self, mock_post_static):
        """Test that the identify method calls the RequestHelper with the correct arguments."""
        # Arrange
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "ok"}
        mock_post_static.return_value = mock_response

        api_key = "test_api_key"

        # Act
        response = self.identity.identify(api_key=api_key)

        # Assert
        expected_url = "https://api.permutive.com/v2.0/identify"

        expected_payload = {
            "user_id": "test_user",
            "aliases": [
                {"id": "alias1", "tag": "tag1", "priority": 1},
                {"id": "alias2", "tag": "tag2", "priority": 2}
            ]
        }

        mock_post_static.assert_called_once_with(
            api_key=api_key,
            url=expected_url,
            data=expected_payload
        )

        self.assertEqual(response, mock_response)


if __name__ == '__main__':
    unittest.main()
