import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import requests
import pandas as pd
import time
import logging
from utils import get_api_key 

def get_weather_data(lat, lon, api_key):
    """Fetch weather data from OpenWeatherMap API."""
    try:
        base_url = "http://api.openweathermap.org/data/2.5/weather"
        params = {
            'lat': lat,
            'lon': lon,
            'appid': api_key,
            'units': 'metric'
        }
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        logging.info(f"Retrieved weather data for lat={lat}, lon={lon}")
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")
    except Exception as e:
        logging.error(f"Error retrieving weather data for lat={lat}, lon={lon}: {e}")
    return None

def process_city_weather(city, api_key):
    """Process weather data for a city."""
    city_weather = get_weather_data(city["lat"], city["lon"], api_key)
    if city_weather is None:
        return None

    return {
        "country": city["country"],
        "capital_city": city["capital_city"],
        "temperature": city_weather["main"]["temp"],
        "feels_like": city_weather["main"]["feels_like"],
        "min_temperature": city_weather["main"]["temp_min"],
        "max_temperature": city_weather["main"]["temp_max"],
        "pressure": city_weather["main"]["pressure"],
        "humidity": city_weather["main"]["humidity"],
        "visibility": city_weather.get("visibility", "N/A"),
        "wind_speed": city_weather["wind"]["speed"],
        "wind_deg": city_weather["wind"]["deg"],
        "weather": city_weather["weather"][0]["description"]
    }

def extract_weather_data(input_cities_file, pause_duration):
    """Extract weather data for cities."""

    api_key = get_api_key('OPENWEATHERMAP')

    try:
        cities_df = pd.read_csv(input_cities_file)
        weather_data = []
        
        for _, city in cities_df.iterrows():
            logging.info(f"Processing weather data for {city['capital_city']}, {city['country']}")
            city_weather = process_city_weather(city, api_key)
            if city_weather:
                weather_data.append(city_weather)
            time.sleep(pause_duration)

        # Save extracted weather data to Parquet file
        weather_df = pd.DataFrame(weather_data)
        logging.info("Extract process completed successfully")
        return weather_df
    except FileNotFoundError:
        logging.error(f"File not found: {input_cities_file}")
        raise
    except Exception as e:
        logging.error(f"An error occurred during extraction: {e}")
        raise