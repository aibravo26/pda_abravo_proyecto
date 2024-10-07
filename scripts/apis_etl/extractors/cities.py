import pandas as pd
import logging

def extract_cities_data(cities_file_path):
    try:
        # Read the CSV file into a DataFrame
        cities_df = pd.read_csv(cities_file_path)
        logging.info(f"Successfully extracted cities data from {cities_file_path}")
        return cities_df
    except Exception as e:
        logging.error(f"Error extracting cities data from {cities_file_path}: {e}")
        raise