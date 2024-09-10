import os
import sys
import unittest
import pandas as pd
from unittest.mock import patch
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from etl.extractors.weather_api import get_weather_data

class TestWeatherData(unittest.TestCase):
    @patch('etl.extractors.weather_api.requests.get')
    def test_get_weather_data(self, mock_get):
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
            },
            "coord": {
                "lon": -0.1257,
                "lat": 51.5085
            }
        }

        # Configure the mock to return a response with our mock_response data
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response

        # Test data
        lat = 51.5085
        lon = -0.1257
        api_key = "dummy_key"

        # Call the function
        result = get_weather_data(lat, lon, api_key)

        # Convert the result to a DataFrame
        result_df = pd.DataFrame([{
            "temperature": result["main"]["temp"],
            "feels_like": result["main"]["feels_like"],
            "min_temperature": result["main"]["temp_min"],
            "max_temperature": result["main"]["temp_max"],
            "pressure": result["main"]["pressure"],
            "humidity": result["main"]["humidity"],
            "visibility": result.get("visibility", "N/A"),
            "wind_speed": result["wind"]["speed"],
            "wind_deg": result["wind"]["deg"],
            "wind_gust": result["wind"].get("gust", "N/A"),
            "weather": result["weather"][0]["description"],
            "weather_main": result["weather"][0]["main"],
            "cloudiness": result["clouds"]["all"],
            "sunrise": result["sys"]["sunrise"],
            "sunset": result["sys"]["sunset"],
            "lon": result["coord"]["lon"],
            "lat": result["coord"]["lat"]
        }])

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
            "sunset": 1622581200,
            "lon": lon,
            "lat": lat
        }])

        # Compare the DataFrames
        pd.testing.assert_frame_equal(result_df, expected_result_df)

if __name__ == '__main__':
    unittest.main(exit=False)