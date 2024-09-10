import os
import sys
import logging
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from etl.extractors.weather_api import extract_weather_data
from etl.transformers.transform_functions import transform_weather_data
from etl.loaders.load_to_redshift import save_to_redshift
from utils.utils import load_config, ensure_output_directory_exists, setup_logging
from utils.redshift_connection import connect_to_redshift

def main():
    setup_logging()

    # Load configuration
    config = load_config()

    # Ensure output directory exists
    ensure_output_directory_exists(config['output_directory'])

    logging.info("Starting the ETL process.")

    # Connect to Redshift
    redshift_engine = connect_to_redshift()

    # Extract Phase: Load cities data, process weather data, and save to a Parquet file
    extracted_weather_file = extract_weather_data(
        config['input_cities_file'], 
        config['extracted_weather_file'], 
        config['pause_duration']
    )

    # Transform Phase: Add new columns and save the transformed data
    transformed_weather_file = transform_weather_data(extracted_weather_file, config['transformed_weather_file'])

    # Load Phase: Save the final processed data to the output Parquet file and return the file path
    save_to_redshift(transformed_weather_file, 'staging_api_weather_data', redshift_engine)

    logging.info("ETL process completed successfully.")

if __name__ == "__main__":
    main()