import io
import unittest
import pandas as pd
from unittest.mock import patch, MagicMock, mock_open
from datetime import datetime
from main import load_config, get_weather_data, get_api_key, load_cities_data

class TestWeatherDataFunctions(unittest.TestCase):

    @patch('main.requests.get')
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


    @patch('main.configparser.ConfigParser')
    def test_load_config(self, mock_config_parser):
        # Mocking the configparser.ConfigParser instance
        mock_config = MagicMock()
        
        # Mocking the 'DEFAULT' section access
        mock_config.__getitem__.return_value = {
            'input_file': 'test_input.csv',
            'output_file': 'test_output.csv',
            'pause_duration': '2.5'
        }
        
        mock_config_parser.return_value = mock_config

        # Call the load_config function
        result = load_config('test_config.ini')

        # Expected output
        expected_result = {
            'input_file': 'test_input.csv',
            'output_file': 'test_output.csv',
            'pause_duration': 2.5
        }

        # Assertions to check if the result matches the expected output
        self.assertEqual(result, expected_result)
        mock_config.read.assert_called_once_with('test_config.ini')


    @patch('main.os.getenv')
    def test_get_api_key_success(self, mock_getenv):
        # Simulate the environment variable being set
        mock_getenv.return_value = 'dummy_api_key'

        # Call the function
        result = get_api_key()

        # Check that the function returns the correct API key
        self.assertEqual(result, 'dummy_api_key')


    @patch('main.os.getenv')
    def test_get_api_key_not_set(self, mock_getenv):
        # Simulate the environment variable not being set
        mock_getenv.return_value = None

        # Check that the function raises a ValueError
        with self.assertRaises(ValueError) as context:
            get_api_key()

        self.assertEqual(str(context.exception), "No API key found. Please set the OPENWEATHERMAP_API_KEY environment variable.")


    @patch('builtins.open', new_callable=mock_open)
    def test_load_cities_data_success(self, mock_file):
        # Mocking a CSV file content
        mock_csv_content = (
            "country,capital,lat,lon\n"
            "argentina,buenos aires,34.2,52.1\n"
            "francia,paris,13.1,22.3\n"
        )

        # Set the mock to return the CSV content when read
        mock_file.return_value = io.StringIO(mock_csv_content)

        # Call the function
        result = load_cities_data('test_file.csv')

        # Expected DataFrame
        expected_df = pd.DataFrame({
            'country': ['argentina', 'francia'],
            'capital': ['buenos aires', 'paris'],
            'lat': [34.2, 13.1],
            'lon': [52.1, 22.3]
        })

        # Check that the function returns the correct DataFrame
        pd.testing.assert_frame_equal(result, expected_df)


if __name__ == '__main__':
    unittest.main(exit=False)