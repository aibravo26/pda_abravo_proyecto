from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import os
import sys
# Insert your project directory to the system path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from apis_etl.extractors.cities import extract_cities_data
from apis_etl.extractors.weather_api import extract_weather_data
from apis_etl.extractors.population_api import extract_population_data
from apis_etl.transformers.transform_functions import transform_execution_dates_addition
from apis_etl.loaders.load_to_redshift import save_to_redshift
from apis_etl.utils import load_config, setup_logging, connect_to_redshift
from dags.database.db_initialization import create_tables_if_not_exist as create_redshift_tables
from dags.database.dim_cities import check_new_citites_additions
from dags.database.dim_population import check_population_updates
from dags.database.fact_weather_metrics import load_incremental_weather_data

# Define the default_args for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 12),
    'retries': 1,
}

# Define a helper function to pull XCom and Redshift connection
def get_config_and_redshift(ti):
    """Helper to get config and Redshift engine."""
    config = ti.xcom_pull(task_ids='load_config')
    redshift_engine = connect_to_redshift()
    return config, redshift_engine

# Define the DAG
with DAG(
    dag_id='etl_process_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    def load_config_task(**kwargs):
        """Step 1: Load configuration."""
        setup_logging()
        config = load_config()
        logging.info("Configuration loaded.")
        return config
    
    def extract_transform_load_cities(**kwargs):
        """Step 2: Extract, transform, and load cities data."""
        ti = kwargs['ti']
        config, redshift_engine = get_config_and_redshift(ti)
        try:
            extracted_cities_df = extract_cities_data(config['input_cities_file'])
            transformed_cities_df = transform_execution_dates_addition(extracted_cities_df, 'cities')
            save_to_redshift(transformed_cities_df, 'staging_cities', redshift_engine)
            logging.info("Cities data loaded into Redshift.")
        except Exception as e:
            logging.error(f"Failed to load cities data: {e}")
            raise

    def extract_transform_load_weather(**kwargs):
        """Step 3: Extract, transform, and load weather data."""
        ti = kwargs['ti']
        config, redshift_engine = get_config_and_redshift(ti)
        try:
            extracted_weather_df = extract_weather_data(config['input_cities_file'], config['pause_duration'])
            transformed_weather_df = transform_execution_dates_addition(extracted_weather_df, 'weather')
            save_to_redshift(transformed_weather_df, 'staging_api_weather_data', redshift_engine)
            logging.info("Weather data loaded into Redshift.")
        except Exception as e:
            logging.error(f"Failed to load weather data: {e}")
            raise

    def extract_transform_load_population(**kwargs):
        """Step 4: Extract, transform, and load population data."""
        ti = kwargs['ti']
        config, redshift_engine = get_config_and_redshift(ti)
        try:
            extracted_population_df = extract_population_data(config['input_cities_file'], config['pause_duration'])
            transformed_population_df = transform_execution_dates_addition(extracted_population_df, 'population')
            save_to_redshift(transformed_population_df, 'staging_api_population_data', redshift_engine)
            logging.info("Population data loaded into Redshift.")
        except Exception as e:
            logging.error(f"Failed to load population data: {e}")
            raise

    def initialize_db(**kwargs):
        """Step 5: Initialize Redshift database tables if they don't exist."""
        redshift_engine = connect_to_redshift()
        try:
            create_redshift_tables(redshift_engine)
            logging.info("Redshift tables initialized.")
        except Exception as e:
            logging.error(f"Failed to initialize Redshift tables: {e}")
            raise

    def process_dim_cities(**kwargs):
        """Step 6: Process dimensional data for cities."""
        redshift_engine = connect_to_redshift()
        try:
            check_new_citites_additions(redshift_engine)
            logging.info("Processed new city additions.")
        except Exception as e:
            logging.error(f"Failed to process city additions: {e}")
            raise

    def process_dim_population(**kwargs):
        """Step 7: Process dimensional data for population."""
        redshift_engine = connect_to_redshift()
        try:
            check_population_updates(redshift_engine)
            logging.info("Processed population updates.")
        except Exception as e:
            logging.error(f"Failed to process population updates: {e}")
            raise

    def process_fact_weather_metrics(**kwargs):
        """Step 8: Load incremental weather data into fact table."""
        redshift_engine = connect_to_redshift()
        try:
            load_incremental_weather_data(redshift_engine)
            logging.info("Loaded incremental weather data.")
        except Exception as e:
            logging.error(f"Failed to load weather data: {e}")
            raise

    # Define the tasks
    load_config_op = PythonOperator(
        task_id='load_config',
        python_callable=load_config_task
    )

    etl_cities_op = PythonOperator(
        task_id='extract_transform_load_cities',
        python_callable=extract_transform_load_cities
    )

    etl_weather_op = PythonOperator(
        task_id='extract_transform_load_weather',
        python_callable=extract_transform_load_weather
    )

    etl_population_op = PythonOperator(
        task_id='extract_transform_load_population',
        python_callable=extract_transform_load_population
    )

    initialize_db_op = PythonOperator(
        task_id='initialize_db',
        python_callable=initialize_db
    )

    process_dim_cities_op = PythonOperator(
        task_id='process_dim_cities',
        python_callable=process_dim_cities
    )

    process_dim_population_op = PythonOperator(
        task_id='process_dim_population',
        python_callable=process_dim_population
    )

    process_fact_weather_metrics_op = PythonOperator(
        task_id='process_fact_weather_metrics',
        python_callable=process_fact_weather_metrics
    )

    # Define task dependencies
    load_config_op >> etl_cities_op >> etl_weather_op >> etl_population_op >> initialize_db_op \
    >> process_dim_cities_op >> process_dim_population_op >> process_fact_weather_metrics_op
