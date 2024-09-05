import os
import sys
import unittest
from unittest.mock import patch
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from main import get_api_key

class TestApiKeySuccess(unittest.TestCase):
    @patch('main.os.getenv')
    def test_get_api_key_success(self, mock_getenv):
        # Simulate the environment variable being set
        mock_getenv.return_value = 'dummy_api_key'

        # Call the function
        result = get_api_key()

        # Check that the function returns the correct API key
        self.assertEqual(result, 'dummy_api_key')

if __name__ == '__main__':
    unittest.main(exit=False)