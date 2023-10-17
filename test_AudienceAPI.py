import unittest
import os
from unittest.mock import patch
# Replace 'your_module' with the actual module name where AudienceAPI resides
from PermutiveAPI.AudienceAPI import AudienceAPI
from PermutiveAPI.Workspace import WorkspaceList

from dotenv import load_dotenv
import logging
logging.getLogger().setLevel(logging.INFO)
load_dotenv()
ws_list = WorkspaceList.read_json()
masterKey = ws_list.get_MasterprivateKey()


class TestAudienceAPI(unittest.TestCase):

    @patch('PermutiveAPI.APIRequestHandler.APIRequestHandler.get')
    def test_list_imports(self, mock_get):
        # Arrange
        mock_get.return_value.json.return_value = {
            'items': [
                # Replace with actual fields expected in the response
                {'name': 'name1', 'id': 'id1', "code": "code1",
                    "relation": None, "identifiers": ["xID"]},
                {'name': 'name2', 'id': 'id2', "code": "code2",
                    "relation": None, "identifiers": ["xID"]}
            ]
        }
        # Initialize as per your requirements
        api = AudienceAPI(api_key=masterKey)

        # Act
        result = api.list_imports()

        # Assert
        self.assertEqual(len(result), 2)
        # Replace 'field1' with actual field names
        self.assertEqual(result[0].name, 'name1')
        # Replace 'field2' with actual field names
        self.assertEqual(result[0].id, 'id1')
        # Replace 'field1' with actual field names
        self.assertEqual(result[1].name, 'name2')
        # Replace 'field2' with actual field names
        self.assertEqual(result[1].id, 'id2')


if __name__ == '__main__':
    unittest.main()
