import unittest
import main
from unittest.mock import patch
from datetime import datetime

class TestWeatherDataRetrieval(unittest.TestCase):

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
        api_key = "dummy_key"  # Replace with a real API key if needed

        # Call the function
        result = main.get_weather_data(lat, lon, api_key)

        # Get the current timestamp and date for comparison
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        date_only = datetime.now().strftime('%Y-%m-%d')

        # Expected result based on the mock_response
        expected_result = {
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
        }

        # Test the output of your function against the expected result
        self.assertEqual(result["main"]["temp"], expected_result["temperature"])
        self.assertEqual(result["main"]["feels_like"], expected_result["feels_like"])
        self.assertEqual(result["main"]["temp_min"], expected_result["min_temperature"])
        self.assertEqual(result["main"]["temp_max"], expected_result["max_temperature"])
        self.assertEqual(result["main"]["pressure"], expected_result["pressure"])
        self.assertEqual(result["main"]["humidity"], expected_result["humidity"])
        self.assertEqual(result.get("visibility", "N/A"), expected_result["visibility"])
        self.assertEqual(result["wind"]["speed"], expected_result["wind_speed"])
        self.assertEqual(result["wind"]["deg"], expected_result["wind_deg"])
        self.assertEqual(result["wind"].get("gust", "N/A"), expected_result["wind_gust"])
        self.assertEqual(result["weather"][0]["description"], expected_result["weather"])
        self.assertEqual(result["weather"][0]["main"], expected_result["weather_main"])
        self.assertEqual(result["clouds"]["all"], expected_result["cloudiness"])
        self.assertEqual(result["sys"]["sunrise"], expected_result["sunrise"])
        self.assertEqual(result["sys"]["sunset"], expected_result["sunset"])
        self.assertEqual(result["coord"]["lon"], expected_result["lon"])
        self.assertEqual(result["coord"]["lat"], expected_result["lat"])

if __name__ == '__main__':
    unittest.main(exit=False)