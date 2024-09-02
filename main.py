import requests
import pandas as pd
import configparser
import logging
import time
import os
from datetime import datetime

# Function to load configuration from the config file
def load_config(config_file='config.ini'):
    config = configparser.ConfigParser()
    config.read(config_file)
    return {
        'input_file': config['DEFAULT']['input_file'],
        'output_file': config['DEFAULT']['output_file'],
        'pause_duration': float(config['DEFAULT']['pause_duration'])
    }

# Function to get API key from environment
def get_api_key():
    api_key = os.getenv('OPENWEATHERMAP_API_KEY')
    if not api_key:
        raise ValueError("No API key found. Please set the OPENWEATHERMAP_API_KEY environment variable.")
    return api_key

# Function to load cities data from CSV
def load_cities_data(input_file):
    try:
        return pd.read_csv(input_file)
    except FileNotFoundError:
        logging.error(f"File not found: {input_file}")
        raise
    except Exception as e:
        logging.error(f"An error occurred while loading the file: {input_file}, Error: {e}")
        raise

# Function to get current weather data from OpenWeatherMap API
def get_weather_data(lat, lon, api_key):
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
        logging.error(f"Error retrieving weather data for lat={lat}, lon={lon}, Error: {e}")
    return None

# Function to process weather data for a city
def process_city_weather(city, api_key):
    city_weather = get_weather_data(city["lat"], city["lon"], api_key)
    if city_weather is None:
        return None
    
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    date = datetime.now().strftime('%Y-%m-%d')
    
    return {
        "timestamp": timestamp,
        "date": date,
        "country": city["country"],
        "capital_city": city["capital_city"],
        "temperature": city_weather["main"]["temp"],
        "feels_like": city_weather["main"]["feels_like"],
        "min_temperature": city_weather["main"]["temp_min"],
        "max_temperature": city_weather["main"]["temp_max"],
        "pressure": city_weather["main"]["pressure"],
        "humidity": city_weather["main"]["humidity"],
        "sea_level": city_weather["main"].get("sea_level"),
        "grnd_level": city_weather["main"].get("grnd_level"),
        "visibility": city_weather.get("visibility", "N/A"),
        "wind_speed": city_weather["wind"]["speed"],
        "wind_deg": city_weather["wind"]["deg"],
        "wind_gust": city_weather["wind"].get("gust", "N/A"),
        "weather": city_weather["weather"][0]["description"],
        "weather_main": city_weather["weather"][0]["main"],
        "cloudiness": city_weather["clouds"]["all"],
        "sunrise": city_weather["sys"]["sunrise"],
        "sunset": city_weather["sys"]["sunset"],
        "lon": city["lon"],
        "lat": city["lat"]
    }

# Function to save the weather data to a CSV file
def save_weather_data(weather_data, output_file):
    if not weather_data:
        logging.warning("No weather data was retrieved.")
        return
    
    weather_df = pd.DataFrame(weather_data)
    weather_df.to_csv(output_file, index=False)
    logging.info(f"Weather data successfully saved to: {output_file}")
    print(weather_df)

# Main function
def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Load configurations and API key
    config = load_config()
    api_key = get_api_key()
    
    logging.info(f"Starting the weather data retrieval process.")
    logging.info(f"Input file: {config['input_file']}")
    logging.info(f"Output file: {config['output_file']}")
    logging.info(f"Pause duration between API requests: {config['pause_duration']} seconds")
    
    # Load cities data
    cities_df = load_cities_data(config['input_file'])
    logging.info(f"Successfully loaded the input file: {config['input_file']}")
    
    # Process each city's weather data
    weather_data = []
    for _, city in cities_df.iterrows():
        logging.info(f"Processing weather data for {city['capital_city']}, {city['country']}")
        city_weather = process_city_weather(city, api_key)
        if city_weather:
            weather_data.append(city_weather)
        time.sleep(config['pause_duration'])
    
    # Save the collected weather data
    save_weather_data(weather_data, config['output_file'])

if __name__ == "__main__":
    main()