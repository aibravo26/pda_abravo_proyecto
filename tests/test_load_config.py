"""
This module contains unit tests for the load_config function from the scripts.apis_etl.utils module.
It tests the correct loading of configuration settings from a configuration file.
"""

import os  # Standard library import
import sys  # Standard library import
import unittest  # Standard library import
from unittest.mock import patch, MagicMock  # Standard library import for mocking

# Insert your project directory into the system path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.apis_etl.utils import load_config  # Local application import

class TestLoadConfig(unittest.TestCase):
    """
    Unit tests for the load_config function from the scripts.apis_etl.utils module.
    """

    @patch('utils.configparser.ConfigParser')
    def test_load_config(self, mock_config_parser):
        """
        Test that load_config reads configuration values correctly and converts them to the appropriate 
        data types.

        Args:
            mock_config_parser (unittest.mock.Mock): Mock object for configparser.ConfigParser.

        Returns:
            None
        """
        # Mocking the configparser.ConfigParser instance
        mock_config = MagicMock()
        
        # Mocking the 'DEFAULT' section access
        mock_config.__getitem__.return_value = {
            'input_cities_file': 'test_input.csv',
            'pause_duration': '2.5'
        }
        
        mock_config_parser.return_value = mock_config

        # Call the load_config function
        result = load_config('test_config.ini')

        # Expected output
        expected_result = {
            'input_cities_file': 'test_input.csv',
            'pause_duration': 2.5
        }

        # Assertions to check if the result matches the expected output
        self.assertEqual(result, expected_result)

        # Ensure that the correct file was read
        mock_config.read.assert_called_once_with('test_config.ini')

if __name__ == '__main__':
    unittest.main(exit=False)
