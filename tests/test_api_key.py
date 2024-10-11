"""
This module contains unit tests for the get_api_key function from the scripts.apis_etl.utils module.
It tests whether the function correctly retrieves API keys from environment variables and handles 
cases where the keys are missing.
"""

import os  # Standard library import
import sys  # Standard library import
import unittest  # Standard library import
from unittest.mock import patch  # Standard library import for mocking

# Insert your project directory into the system path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Correct import of get_api_key, moved to the top
from scripts.apis_etl.utils import get_api_key  # Local application import

class TestApiKeySuccess(unittest.TestCase):
    """
    Unit tests for the get_api_key function to verify correct retrieval of API keys from environment 
    variables.
    """

    @patch('scripts.apis_etl.utils.os.getenv')  # Corrected the patch path
    def test_get_api_key_success(self, mock_getenv):
        """
        Test that get_api_key successfully retrieves the API key when the corresponding environment 
        variable is set.

        Args:
            mock_getenv (unittest.mock.Mock): Mock object for os.getenv.

        Returns:
            None
        """
        # Simulate the environment variable being set
        mock_getenv.return_value = 'dummy_api_key'

        # Call the function with a specific API name (e.g., 'OPENWEATHERMAP')
        result = get_api_key('OPENWEATHERMAP')

        # Check that the function returns the correct API key
        self.assertEqual(result, 'dummy_api_key')

        # Check that the correct environment variable is used
        mock_getenv.assert_called_with('OPENWEATHERMAP_API_KEY')

    @patch('os.getenv')
    def test_get_api_key_not_set(self, mock_getenv):
        """
        Test that get_api_key raises a ValueError when the corresponding environment
        variable is not set.

        Args:
            mock_getenv (unittest.mock.Mock): Mock object for os.getenv.

        Returns:
            None
        """
        # Simulate the environment variable not being set
        mock_getenv.return_value = None

        # Check that the function raises a ValueError
        with self.assertRaises(ValueError) as context:
            get_api_key('OPENWEATHERMAP')

        # Check that the correct error message is raised
        self.assertEqual(
            str(context.exception),
            "No API key found for OPENWEATHERMAP. "
            "Please set the OPENWEATHERMAP_API_KEY environment variable."
        )

        # Check that the correct environment variable was checked
        mock_getenv.assert_called_with('OPENWEATHERMAP_API_KEY')

if __name__ == '__main__':
    unittest.main(exit=False)
