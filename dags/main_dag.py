from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import os
import sys
# Insert your project directory to the system path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.apis_etl.extractors.cities import extract_cities_data
from scripts.apis_etl.extractors.weather_api import extract_weather_data
from scripts.apis_etl.extractors.population_api import extract_population_data
from scripts.apis_etl.transformers.transform_functions import transform_execution_dates_addition
from scripts.apis_etl.loaders.load_to_redshift import save_to_redshift
from scripts.apis_etl.utils import load_config, setup_logging, connect_to_redshift
from scripts.database.db_initialization import create_tables_if_not_exist as create_redshift_tables
from scripts.database.dim_cities import check_new_citites_additions
from scripts.database.dim_population import check_population_updates
from scripts.database.fact_weather_metrics import load_incremental_weather_data

# Define the default_args for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 18),
    'retries': 1,
}

# Define a helper function to pull XCom and Redshift connection
def get_config_and_redshift(ti):
    """Helper to get config and Redshift engine."""
    config = ti.xcom_pull(task_ids='load_config')
    redshift_engine = connect_to_redshift()
    return config, redshift_engine

def load_config_task(**kwargs):
    """Load configuration."""
    setup_logging()
    config = load_config()
    logging.info("Configuration loaded.")
    return config

def extract_transform_load_generic(extract_func, transform_type, table_name, requires_pause_duration=False, **kwargs):
    """Generic function for extracting, transforming, and loading data into Redshift."""
    ti = kwargs['ti']
    config, redshift_engine = get_config_and_redshift(ti)
    try:
        # Extract data using the provided extract function
        if requires_pause_duration:
            extracted_df = extract_func(config['input_cities_file'], config.get('pause_duration', None))
        else:
            extracted_df = extract_func(config['input_cities_file'])
        
        # Transform the data
        transformed_df = transform_execution_dates_addition(extracted_df, transform_type)
        
        # Load the transformed data into Redshift
        save_to_redshift(transformed_df, table_name, redshift_engine)
        logging.info(f"{transform_type.capitalize()} data loaded into {table_name} in Redshift.")
        
    except Exception as e:
        logging.error(f"Failed to load {transform_type} data: {e}")
        raise

def initialize_db(**kwargs):
    """Initialize Redshift database tables if they don't exist."""
    redshift_engine = connect_to_redshift()
    try:
        create_redshift_tables(redshift_engine)
        logging.info("Redshift tables initialized.")
    except Exception as e:
        logging.error(f"Failed to initialize Redshift tables: {e}")
        raise

def process_dim_cities(**kwargs):
    """Process dimensional data for cities."""
    redshift_engine = connect_to_redshift()
    try:
        check_new_citites_additions(redshift_engine)
        logging.info("Processed new city additions.")
    except Exception as e:
        logging.error(f"Failed to process city additions: {e}")
        raise

def process_dim_population(**kwargs):
    """Process dimensional data for population."""
    redshift_engine = connect_to_redshift()
    try:
        check_population_updates(redshift_engine)
        logging.info("Processed population updates.")
    except Exception as e:
        logging.error(f"Failed to process population updates: {e}")
        raise

def process_fact_weather_metrics(**kwargs):
    """Load incremental weather data into fact table."""
    redshift_engine = connect_to_redshift()
    try:
        load_incremental_weather_data(redshift_engine)
        logging.info("Loaded incremental weather data.")
    except Exception as e:
        logging.error(f"Failed to load weather data: {e}")
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

    etl_cities_op = PythonOperator(
        task_id='extract_transform_load_cities',
        python_callable=extract_transform_load_generic,
        op_kwargs={
            'extract_func': extract_cities_data,
            'transform_type': 'cities',
            'table_name': 'staging_cities',
            'requires_pause_duration': False
        }
    )

    etl_weather_op = PythonOperator(
        task_id='extract_transform_load_weather',
        python_callable=extract_transform_load_generic,
        op_kwargs={
            'extract_func': extract_weather_data,
            'transform_type': 'weather',
            'table_name': 'staging_api_weather_data',
            'requires_pause_duration': True
        }
    )

    etl_population_op = PythonOperator(
        task_id='extract_transform_load_population',
        python_callable=extract_transform_load_generic,
        op_kwargs={
            'extract_func': extract_population_data,
            'transform_type': 'population',
            'table_name': 'staging_api_population_data',
            'requires_pause_duration': True
        }
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
