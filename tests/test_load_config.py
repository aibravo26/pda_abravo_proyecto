import os
import sys
import unittest
import pandas as pd
from unittest.mock import patch, MagicMock
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.utils import load_config

class TestLoadConfig(unittest.TestCase):
    @patch('utils.utils.configparser.ConfigParser')
    def test_load_config(self, mock_config_parser):
        # Mocking the configparser.ConfigParser instance
        mock_config = MagicMock()
        
        # Mocking the 'DEFAULT' section access
        mock_config.__getitem__.return_value = {
            'input_cities_file': 'test_input.csv',
            'output_directory': 'etl_output_files',
            'extracted_weather_file': 'test_extracted.parquet',
            'transformed_weather_file': 'test_transformed.parquet',
            'loaded_weather_file': 'test_loaded.parquet',
            'pause_duration': '2.5'
        }
        
        mock_config_parser.return_value = mock_config

        # Call the load_config function
        result = load_config('test_config.ini')

        # Expected output
        expected_result = {
            'input_cities_file': 'test_input.csv',
            'output_directory': 'etl_output_files',
            'extracted_weather_file': 'test_extracted.parquet',
            'transformed_weather_file': 'test_transformed.parquet',
            'loaded_weather_file': 'test_loaded.parquet',
            'pause_duration': 2.5
        }

        # Assertions to check if the result matches the expected output
        self.assertEqual(result, expected_result)
        mock_config.read.assert_called_once_with('test_config.ini')

if __name__ == '__main__':
    unittest.main(exit=False)