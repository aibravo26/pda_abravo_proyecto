import os
import sys
import unittest
from unittest.mock import patch
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dags.apis_etl.utils import get_api_key

class TestApiKeySuccess(unittest.TestCase):
    @patch('dags.apis_etl.utils.os.getenv')
    def test_get_api_key_success(self, mock_getenv):
        # Simulate the environment variable being set
        mock_getenv.return_value = 'dummy_api_key'

        # Call the function with a specific API name (e.g., 'OPENWEATHERMAP')
        result = get_api_key('OPENWEATHERMAP')

        # Check that the function returns the correct API key
        self.assertEqual(result, 'dummy_api_key')

        # Check that the correct environment variable is used
        mock_getenv.assert_called_with('OPENWEATHERMAP_API_KEY')

    @patch('main.os.getenv')
    def test_get_api_key_not_set(self, mock_getenv):
        # Simulate the environment variable not being set
        mock_getenv.return_value = None

        # Check that the function raises a ValueError
        with self.assertRaises(ValueError) as context:
            get_api_key('OPENWEATHERMAP')

        # Check that the correct error message is raised
        self.assertEqual(str(context.exception), "No API key found for OPENWEATHERMAP. Please set the OPENWEATHERMAP_API_KEY environment variable.")

        # Check that the correct environment variable was checked
        mock_getenv.assert_called_with('OPENWEATHERMAP_API_KEY')       

if __name__ == '__main__':
    unittest.main(exit=False)