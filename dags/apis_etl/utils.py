import os
import configparser
import logging
from sqlalchemy import create_engine

def load_config(config_file='config/config.ini'):
    """Load configuration from the config file."""
    config = configparser.ConfigParser()
    config.read(config_file)

    return {
        'input_cities_file': config['DEFAULT']['input_cities_file'],
        'pause_duration': float(config['DEFAULT']['pause_duration']),
    }

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

def connect_to_redshift():
    """Connect to Redshift using SQLAlchemy."""
    try:
        redshift_host = os.getenv('REDSHIFT_HOST')
        redshift_port = os.getenv('REDSHIFT_PORT')
        redshift_dbname = os.getenv('REDSHIFT_DBNAME')
        redshift_user = os.getenv('REDSHIFT_USER')
        redshift_password = os.getenv('REDSHIFT_PASSWORD')

        connection_string = (
            f'postgresql+psycopg2://{redshift_user}:{redshift_password}'
            f'@{redshift_host}:{redshift_port}/{redshift_dbname}'
        )

        engine = create_engine(connection_string, connect_args={"options": ""})
        logging.info("Successfully connected to Redshift using SQLAlchemy")
        return engine
    except Exception as e:
        logging.error(f"Error connecting to Redshift: {e}")
        raise
