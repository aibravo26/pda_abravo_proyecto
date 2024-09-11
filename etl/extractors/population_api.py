import os
import sys
import logging
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import requests
import pandas as pd
import time
from utils import get_api_key

def get_population_data(lat, lon, username):
    """Retrieve population data from GeoNames API based on latitude and longitude."""
    try:
        base_url = "http://api.geonames.org/findNearbyPlaceNameJSON"
        params = {
            'lat': lat,
            'lng': lon,
            'username': username
        }
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()

        if 'geonames' in data and len(data['geonames']) > 0:
            population_str = data['geonames'][0].get('population', 'N/A')

            # Validate and handle population data
            try:
                population = int(population_str)
            except (ValueError, TypeError):
                logging.warning(f"Invalid population data for lat={lat}, lon={lon}. Setting population to None.")
                population = None  # Or set to 0 if that's preferable

            logging.info(f"Retrieved population data for lat={lat}, lon={lon}: {population}")
            return population
        else:
            return None
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")
        return None
    except Exception as e:
        logging.error(f"Error retrieving population data for lat={lat}, lon={lon}: {e}")
        return None

def extract_population_data(input_cities_file, pause_duration):
    """Extract population data for cities based on lat/lon coordinates."""
    try:
        # Retrieve the GeoNames username from environment variables using the utility
        username = get_api_key('GEONAMES')
        
        # Load city data from CSV
        cities_df = pd.read_csv(input_cities_file)
        population_data = []

        for _, city in cities_df.iterrows():
            logging.info(f"Processing population data for {city['capital_city']}, {city['country']}")
            population = get_population_data(city['lat'], city['lon'], username)
            population_data.append({
                'country': city['country'],
                'capital_city': city['capital_city'],
                'population': population  # This will be either an integer or None
            })
            time.sleep(pause_duration)

        # Save extracted population data to a Parquet file
        population_df = pd.DataFrame(population_data)
        logging.info("Extract process completed successfully")
        return population_df
    except FileNotFoundError:
        logging.error(f"File not found: {input_cities_file}")
        raise
    except Exception as e:
        logging.error(f"An error occurred during extraction: {e}")
        raise