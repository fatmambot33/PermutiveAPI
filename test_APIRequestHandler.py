import unittest
from unittest.mock import patch

from PermutiveAPI.APIRequestHandler import APIRequestHandler


class TestAPIRequestHandler(unittest.TestCase):

    @patch('requests.get')
    def test_get_method(self, mock_get):
        mock_get.return_value.status_code = 200
        response = APIRequestHandler.get('www.google.com')
        self.assertEqual(response.status_code, 200)


if __name__ == '__main__':
    unittest.main()
