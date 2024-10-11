"""
This module handles population data extraction from the GeoNames API for cities based on
latitude and longitude coordinates. It processes city data, retrieves population information
via the API, and returns the data as a pandas DataFrame.
"""

import os  # Standard Library import
import sys  # Standard Library import
import logging  # Standard Library import
import time  # Standard Library import

import requests  # Third-party import
import pandas as pd  # Third-party import

from scripts.apis_etl.utils import get_api_key  # Local application import

# Insert your project directory to the system path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def get_population_data(lat, lon, username):
    """Retrieve population data from GeoNames API based on latitude and longitude."""
    try:
        base_url = "http://api.geonames.org/findNearbyPlaceNameJSON"
        params = {
            'lat': lat,
            'lng': lon,
            'username': username
        }
        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if 'geonames' in data and len(data['geonames']) > 0:
            population_str = data['geonames'][0].get('population', 'N/A')

            # Validate and handle population data
            try:
                population = int(population_str)
            except (ValueError, TypeError):
                logging.warning(
                    "Invalid population data for lat=%s, lon=%s. Setting population to None.",
                    lat, lon
                )
                population = None

            logging.info(
                "Retrieved population data for lat=%s, lon=%s: %s", 
                lat, lon, population
            )
            return population

        return None

    except requests.exceptions.Timeout:
        logging.error(
            "Timeout occurred while fetching population data for lat=%s, lon=%s", 
            lat, lon
        )
        return None

    except requests.exceptions.ConnectionError:
        logging.error(
            "Connection error occurred while fetching population data for lat=%s, lon=%s", 
            lat, lon
        )
        return None

    except requests.exceptions.HTTPError as http_error:
        logging.error("HTTP error occurred while fetching population data: %s", http_error)
        return None

    except requests.exceptions.RequestException as req_error:
        logging.error("An error occurred while fetching population data: %s", req_error)
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
            logging.info(
                "Processing population data for %s, %s", 
                city['capital_city'], city['country']
            )
            population = get_population_data(city['lat'], city['lon'], username)
            population_data.append({
                'country': city['country'],
                'capital_city': city['capital_city'],
                'population': population  # This will be either an integer or None
            })
            time.sleep(pause_duration)

        population_df = pd.DataFrame(population_data)
        logging.info("Extract process completed successfully")
        return population_df

    except FileNotFoundError:
        raise

    except Exception as error:
        logging.error("An error occurred during extraction: %s", error)
        raise
