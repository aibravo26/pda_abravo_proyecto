"""
This module provides functions to extract and process weather data from the OpenWeatherMap API.
It converts the weather data into pandas DataFrames and handles multiple cities.
"""

import os  # Standard library import
import sys  # Standard library import
import time  # Standard library import
import logging  # Standard library import

import requests  # Third-party import
import pandas as pd  # Third-party import

from scripts.apis_etl.utils import get_api_key  # Local application import

# Insert your project directory to the system path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def convert_weather_data_to_df(weather_data):
    """Convert Weather Dict to DataFrame"""
    return pd.DataFrame([{
        "temperature": weather_data["main"]["temp"],
        "feels_like": weather_data["main"]["feels_like"],
        "min_temperature": weather_data["main"]["temp_min"],
        "max_temperature": weather_data["main"]["temp_max"],
        "pressure": weather_data["main"]["pressure"],
        "humidity": weather_data["main"]["humidity"],
        "visibility": weather_data.get("visibility", "N/A"),
        "wind_speed": weather_data["wind"]["speed"],
        "wind_deg": weather_data["wind"]["deg"],
        "wind_gust": weather_data["wind"].get("gust", "N/A"),
        "weather": weather_data["weather"][0]["description"],
        "weather_main": weather_data["weather"][0]["main"],
        "cloudiness": weather_data["clouds"]["all"],
        "sunrise": weather_data["sys"]["sunrise"],
        "sunset": weather_data["sys"]["sunset"]
    }])

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
        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()
        logging.info("Retrieved weather data for lat=%s, lon=%s", lat, lon)
        return response.json()

    except requests.exceptions.Timeout:
        logging.error(
            "Timeout occurred while fetching weather data for lat=%s, lon=%s", 
            lat, lon
        )
    except requests.exceptions.ConnectionError:
        logging.error(
            "Connection error occurred while fetching weather data for lat=%s, lon=%s", 
            lat, lon
        )
    except requests.exceptions.HTTPError as http_error:
        logging.error("HTTP error occurred: %s", http_error)
    except requests.exceptions.RequestException as req_error:
        logging.error("An error occurred while fetching weather data: %s", req_error)

    return None

def process_city_weather(city, api_key):
    """Process weather data for a city and return as a DataFrame."""
    city_weather = get_weather_data(city["lat"], city["lon"], api_key)
    if city_weather is None:
        return None

    # Convert weather data to DataFrame using the imported function
    weather_df = convert_weather_data_to_df(city_weather)

    # Add additional city information to the DataFrame
    weather_df["country"] = city["country"]
    weather_df["capital_city"] = city["capital_city"]

    return weather_df

def extract_weather_data(input_cities_file, pause_duration):
    """Extract weather data for cities and aggregate into a DataFrame."""

    api_key = get_api_key('OPENWEATHERMAP')

    try:
        cities_df = pd.read_csv(input_cities_file)
        weather_data_frames = []  # List to hold individual DataFrames

        for _, city in cities_df.iterrows():
            logging.info(
                "Processing weather data for %s, %s", 
                city['capital_city'], city['country']
            )
            city_weather_df = process_city_weather(city, api_key)
            if city_weather_df is not None:
                weather_data_frames.append(city_weather_df)
            time.sleep(pause_duration)

        # Concatenate all individual DataFrames into a single DataFrame
        if weather_data_frames:
            weather_df = pd.concat(weather_data_frames, ignore_index=True)
            logging.info("Extract process completed successfully")
            return weather_df

        logging.warning("No weather data extracted.")
        return pd.DataFrame()  # Return an empty DataFrame if no data was processed

    except FileNotFoundError:
        logging.error("File not found: %s", input_cities_file)
        raise
    except Exception as error:
        logging.error("An error occurred during extraction: %s", error)
        raise
