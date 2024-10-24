"""
This module contains unit tests for functions that extract and convert weather data using 
the OpenWeatherMap API. It tests the retrieval of weather data and its conversion to a pandas 
DataFrame.
"""

import os  # Standard library import
import sys  # Standard library import
import unittest  # Standard library import
from unittest.mock import patch  # Standard library import for mocking
import pandas as pd  # Third-party import

# Insert your project directory into the system path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Correct imports, moved to the top of the module
from scripts.apis_etl.extractors.weather_api import (
    get_weather_data, convert_weather_data_to_df
)  # Local imports

class TestWeatherData(unittest.TestCase):
    """
    Unit tests for the weather data extraction and conversion functions from the 
    scripts.apis_etl.extractors.weather_api module.
    """

    @patch('scripts.apis_etl.extractors.weather_api.requests.get')
    def test_get_weather_data(self, mock_get):
        """
        Test that get_weather_data retrieves and converts weather data from the OpenWeatherMap API 
        correctly.

        Args:
            mock_get (unittest.mock.Mock): Mock object for requests.get.

        Returns:
            None
        """
        # Mock response data from OpenWeatherMap API
        mock_response = {
            "main": {
                "temp": 15.0,
                "feels_like": 14.0,
                "temp_min": 10.0,
                "temp_max": 20.0,
                "pressure": 1013,
                "humidity": 80
            },
            "visibility": 10000,
            "wind": {
                "speed": 5.0,
                "deg": 200,
                "gust": 7.0
            },
            "weather": [
                {
                    "description": "clear sky",
                    "main": "Clear"
                }
            ],
            "clouds": {
                "all": 0
            },
            "sys": {
                "sunrise": 1622527200,
                "sunset": 1622581200
            }
        }

        # Configure the mock to return a response with our mock_response data
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response

        # Test data
        lat = 51.5085
        lon = -0.1257
        api_key = "dummy_key"

        # Call the function to get weather data
        result = get_weather_data(lat, lon, api_key)

        # Convert the result using the imported function
        result_df = convert_weather_data_to_df(result)

        # Expected result DataFrame
        expected_result_df = pd.DataFrame([{
            "temperature": 15.0,
            "feels_like": 14.0,
            "min_temperature": 10.0,
            "max_temperature": 20.0,
            "pressure": 1013,
            "humidity": 80,
            "visibility": 10000,
            "wind_speed": 5.0,
            "wind_deg": 200,
            "wind_gust": 7.0,
            "weather": "clear sky",
            "weather_main": "Clear",
            "cloudiness": 0,
            "sunrise": 1622527200,
            "sunset": 1622581200
        }])

        # Compare the DataFrames
        pd.testing.assert_frame_equal(result_df, expected_result_df)

if __name__ == '__main__':
    unittest.main(exit=False)
