import os
import sys
import logging
import pandas as pd
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from etl.extractors.cities import extract_cities_data
from etl.extractors.weather_api import extract_weather_data
from etl.extractors.population_api import extract_population_data
from etl.transformers.transform_functions import transform_execution_dates_addition
from etl.loaders.load_to_redshift import save_to_redshift
from utils import load_config, setup_logging, connect_to_redshift

def main():
    setup_logging()

    # Load configuration
    config = load_config()

    logging.info("Starting the ETL process.")

    # Connect to Redshift
    redshift_engine = connect_to_redshift()

    # Extract Phase: process cities data
    extracted_cities_df = extract_cities_data(config['input_cities_file'])

    # Extract Phase: process weather data
    extracted_weather_df = extract_weather_data(config['input_cities_file'], config['pause_duration'])    

    # Extract Phase: process populatation data
    extracted_population_df = extract_population_data(config['input_cities_file'], config['pause_duration'])

    # Transform Phase: Add execution times to weather data
    transformed_cities_df = transform_execution_dates_addition(extracted_cities_df, 'cities')

    # Transform Phase: Add execution times to weather data
    transformed_weather_df = transform_execution_dates_addition(extracted_weather_df, 'weather')

    # Transform Phase: Add execution times to population data
    transformed_population_df = transform_execution_dates_addition(extracted_population_df, 'population')

    # Load Phase: Save the input cities file to Redshift table 'staging_cities'
    save_to_redshift(transformed_cities_df, 'staging_cities', redshift_engine)

    # Load Phase: Save the final weather processed data as both Parquet file and Redshift table
    save_to_redshift(transformed_weather_df, 'staging_api_weather_data', redshift_engine)

    # Load Phase: Save the final population processed data as both Parquet file and Redshift table
    save_to_redshift(transformed_population_df, 'staging_api_population_data', redshift_engine)

    logging.info("ETL process completed successfully.")

if __name__ == "__main__":
    main()