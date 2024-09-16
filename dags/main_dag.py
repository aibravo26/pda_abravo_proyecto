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
from redshift.tables_creation import create_tables_if_not_exist as create_redshift_tables

# Define the default_args for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 12),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='etl_process_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    def load_config_task(**kwargs):
        """Step 1: Load configuration."""
        global config
        setup_logging()
        config = load_config()
        logging.info("Configuration loaded.")
        return config  # This pushes the config to XCom
    
    def extract_transform_load_cities(**kwargs):
        """Step 3: Extract, transform, and load cities data."""
        ti = kwargs['ti']
        config = ti.xcom_pull(task_ids='load_config')
        redshift_engine = connect_to_redshift()
        extracted_cities_df = extract_cities_data(config['input_cities_file'])
        transformed_cities_df = transform_execution_dates_addition(extracted_cities_df, 'cities')
        save_to_redshift(transformed_cities_df, 'staging_cities', redshift_engine)
        logging.info("Cities data loaded into Redshift.")
    
    def extract_transform_load_weather(**kwargs):
        """Step 4: Extract, transform, and load weather data."""
        ti = kwargs['ti']
        config = ti.xcom_pull(task_ids='load_config')
        redshift_engine = connect_to_redshift()
        config = ti.xcom_pull(task_ids='load_config')  # Pull config from XCom
        extracted_weather_df = extract_weather_data(config['input_cities_file'], config['pause_duration'])
        transformed_weather_df = transform_execution_dates_addition(extracted_weather_df, 'weather')
        save_to_redshift(transformed_weather_df, 'staging_api_weather_data', redshift_engine)
        logging.info("Weather data loaded into Redshift.")
    
    def extract_transform_load_population(**kwargs):
        """Step 5: Extract, transform, and load population data."""
        ti = kwargs['ti']
        config = ti.xcom_pull(task_ids='load_config')
        redshift_engine = connect_to_redshift()
        config = ti.xcom_pull(task_ids='load_config')  # Pull config from XCom
        extracted_population_df = extract_population_data(config['input_cities_file'], config['pause_duration'])
        transformed_population_df = transform_execution_dates_addition(extracted_population_df, 'population')
        save_to_redshift(transformed_population_df, 'staging_api_population_data', redshift_engine)
        logging.info("Population data loaded into Redshift.")

    def create_tables_task(**kwargs):
        """Create Redshift tables if they don't exist."""
        create_redshift_tables()


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

    create_tables_op = PythonOperator(
        task_id='create_tables_if_not_exist',
        python_callable=create_tables_task  # Call the new function name
    )

    # Define task dependencies
    load_config_op >> etl_cities_op >> etl_weather_op >> etl_population_op >> create_tables_op