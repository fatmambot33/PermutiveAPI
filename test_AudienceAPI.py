import unittest
from unittest.mock import patch
from dotenv import load_dotenv
from PermutiveAPI.AudienceAPI import AudienceAPI
import logging

from PermutiveAPI.Workspace import WorkspaceList
logging.getLogger().setLevel(logging.INFO)
load_dotenv()
ws_list = WorkspaceList.read_json()
masterKey = ws_list.get_MasterprivateKey()


class TestAudienceAPI(unittest.TestCase):

    @patch('PermutiveAPI.AudienceAPI.list_imports()')
    def test_list_imports(self, mock_get):
        # Setup
        mock_get.return_value.json.return_value = {'items': [...]}

        # Exercise
        api = AudienceAPI(api_key=masterKey)
        result = api.list_imports()

        # Verify
        self.assertIsNotNone(result)
        # Add more assertions here

    @patch('your_module.APIRequestHandler.get')
    def test_list_segments(self, mock_get):
        # Setup
        mock_get.return_value.json.return_value = {'elements': [...]}

        # Exercise
        api = AudienceAPI(api_key='test_api_key')
        result = api.list_segments(import_id='test_import_id')

        # Verify
        self.assertIsNotNone(result)
        # Add more assertions here

    # Add more tests, such as test_get_segment, test_create_segment, etc.


if __name__ == '__main__':
    unittest.main()
