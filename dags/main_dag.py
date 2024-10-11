"""
This DAG defines the ETL process for loading cities, weather, and population data into Redshift. 
It includes the extraction, transformation, and loading (ETL) steps as well as table initialization 
and updates for Slowly Changing Dimensions (SCD) in Redshift.
"""

import os  # Standard library import
import sys  # Standard library import
from datetime import datetime  # Standard library import
import logging  # Standard library import

from airflow import DAG  # Third-party import
from airflow.operators.python import PythonOperator  # Third-party import

# Insert your project directory to the system path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Imports of custom modules for ETL - move imports to the top of the module
from scripts.apis_etl.extractors.cities import extract_cities_data
from scripts.apis_etl.extractors.weather_api import extract_weather_data
from scripts.apis_etl.extractors.population_api import extract_population_data
from scripts.apis_etl.transformers.transform_functions import (
    transform_execution_dates_addition
)
from scripts.apis_etl.loaders.load_to_redshift import save_to_redshift
from scripts.apis_etl.utils import (
    load_config, setup_logging, connect_to_redshift
)
from scripts.database.db_initialization import create_tables_if_not_exist as create_redshift_tables
from scripts.database.dim_cities import check_new_cities_additions
from scripts.database.dim_population import check_population_updates
from scripts.database.fact_weather_metrics import load_incremental_weather_data

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 1),
    'retries': 1,
}

def get_config_and_redshift(task_instance):
    """Helper to get config and Redshift engine."""
    config = task_instance.xcom_pull(task_ids='load_config')
    redshift_engine = connect_to_redshift()
    return config, redshift_engine

def load_config_task():
    """Load configuration."""
    setup_logging()
    config = load_config()
    logging.info("Configuration loaded.")
    return config

def extract_transform_load_generic(extract_func, transform_type, table_name, requires_pause_duration=False, **kwargs):
    """Generic function for extracting, transforming, and loading data into Redshift."""
    task_instance = kwargs['task_instance']
    config, redshift_engine = get_config_and_redshift(task_instance)
    try:
        # Extract data using the provided extract function
        if requires_pause_duration:
            extracted_df = extract_func(
                config['input_cities_file'], config.get('pause_duration', None)
            )
        else:
            extracted_df = extract_func(config['input_cities_file'])

        # Transform the data
        transformed_df = transform_execution_dates_addition(extracted_df, transform_type)

        # Load the transformed data into Redshift
        save_to_redshift(transformed_df, table_name, redshift_engine)
        logging.info("%s data loaded into %s in Redshift.", transform_type.capitalize(), table_name)

    except Exception as error:
        logging.error("Failed to load %s data: %s", transform_type, error)
        raise

def extract_transform_load_sources(**kwargs):
    """Consolidate ETL steps for cities, weather, and population using the generic function."""
    try:
        extract_transform_load_generic(
            extract_func=extract_cities_data,
            transform_type='cities',
            table_name='staging_cities',
            requires_pause_duration=False,
            **kwargs
        )
        logging.info("Cities data ETL completed.")

        extract_transform_load_generic(
            extract_func=extract_weather_data,
            transform_type='weather',
            table_name='staging_api_weather_data',
            requires_pause_duration=True,
            **kwargs
        )
        logging.info("Weather data ETL completed.")

        extract_transform_load_generic(
            extract_func=extract_population_data,
            transform_type='population',
            table_name='staging_api_population_data',
            requires_pause_duration=True,
            **kwargs
        )
        logging.info("Population data ETL completed.")

    except Exception as error:
        logging.error("Failed to execute consolidated ETL for cities, weather, and population: %s", error)
        raise

def initialize_and_process_db():
    """Initialize Redshift tables and process cities, population, and weather data."""
    redshift_engine = connect_to_redshift()
    try:
        # Initialize tables
        create_redshift_tables(redshift_engine)
        logging.info("Redshift tables initialized.")

        # Process city additions
        check_new_cities_additions(redshift_engine)
        logging.info("Processed new city additions.")

        # Process population updates
        check_population_updates(redshift_engine)
        logging.info("Processed population updates.")

        # Load incremental weather data
        load_incremental_weather_data(redshift_engine)
        logging.info("Loaded incremental weather data.")

    except Exception as error:
        logging.error("Failed to initialize and process data: %s", error)
        raise

# Define the DAG
with DAG(
    dag_id='etl_process_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Define the tasks
    load_config_op = PythonOperator(
        task_id='load_config',
        python_callable=load_config_task
    )

    extract_transform_load_sources_op = PythonOperator(
        task_id='extract_transform_load_sources',
        python_callable=extract_transform_load_sources
    )

    initialize_and_process_db_op = PythonOperator(
        task_id='initialize_and_process_db',
        python_callable=initialize_and_process_db
    )

    # Define task dependencies
    load_config_op >> extract_transform_load_sources_op >> initialize_and_process_db_op
