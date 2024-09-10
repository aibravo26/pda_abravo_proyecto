import configparser
import os
import logging
import logging

def load_config(config_file='config.ini'):
    """Load configuration from the config file."""
    config = configparser.ConfigParser()
    config.read(config_file)

    return {
        'input_cities_file': config['DEFAULT']['input_cities_file'],
        'output_directory': config['DEFAULT']['output_directory'],
        'extracted_weather_file': config['DEFAULT']['extracted_weather_file'],
        'transformed_weather_file': config['DEFAULT']['transformed_weather_file'],
        'loaded_weather_file': config['DEFAULT']['loaded_weather_file'],
        'extracted_population_file': config['DEFAULT']['extracted_population_file'],
        'transformed_population_file': config['DEFAULT']['transformed_population_file'],
        'loaded_population_file': config['DEFAULT']['loaded_population_file'],
        'pause_duration': float(config['DEFAULT']['pause_duration']),
    }

def ensure_output_directory_exists(output_directory):
    """Ensure the output directory exists, create it if not."""
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
        logging.info(f"Created output directory: {output_directory}")

def setup_logging():
    """Setup logging configuration."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_api_key(api_name):
    """
    Retrieve the API key from environment variables.
    
    :param api_name: The name of the API (e.g., "OPENWEATHERMAP", "GEONAMES").
    :return: The API key as a string.
    :raises: ValueError if the API key is not found.
    """
    api_key = os.getenv(f'{api_name}_API_KEY')
    if not api_key:
        logging.error(f"No API key found for {api_name}. Please set the {api_name}_API_KEY environment variable.")
        raise ValueError(f"No API key found for {api_name}. Please set the {api_name}_API_KEY environment variable.")
    logging.info(f"Successfully retrieved API key for {api_name}")
    return api_key