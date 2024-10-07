# Insert your project directory to the system path
import os
import sys
import logging
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def create_tables_if_not_exist(engine):
    """Create dim_cities, dim_population, and fact_weather_metrics tables with identity columns if they don't exist."""

    schema_name = f'"{os.getenv("REDSHIFT_SCHEMA")}"' 

    try:
        logging.info(f"Connecting to Redshift for schema '{schema_name}'.")

        # SQL queries to create tables if they don't exist
        create_dim_cities_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.dim_cities (
            id INT IDENTITY(1, 1),
            city_name VARCHAR(255),
            country VARCHAR(255),
            lat FLOAT,
            lon FLOAT,
            PRIMARY KEY(id)
        );
        """
        logging.info(f"Preparing to create table {schema_name}.dim_cities.")

        create_dim_population_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.dim_population (
            city_id INT,
            population INT,
            effective_date TIMESTAMPTZ,
            expiration_date TIMESTAMPTZ,
            is_current BOOLEAN,
            PRIMARY KEY(city_id, effective_date)
        );
        """
        logging.info(f"Preparing to create table {schema_name}.dim_population.")

        create_fact_weather_metrics_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.fact_weather_metrics (
            city_id INT, 
            temperature FLOAT8,
            feels_like FLOAT8,
            min_temperature FLOAT8,
            max_temperature FLOAT8,
            pressure INT8,
            humidity INT8,
            visibility INT8,
            wind_speed FLOAT8,
            wind_deg INT8,
            weather VARCHAR(256),
            execution_timestamp_utc VARCHAR(256),
            execution_date VARCHAR(256),
            PRIMARY KEY(city_id, execution_timestamp_utc)
        );
        """
        logging.info(f"Preparing to create table {schema_name}.fact_weather_metrics.")

        # Execute the queries
        with engine.connect() as connection:
            logging.info(f"Executing table creation queries in schema '{schema_name}'.")
            connection.execute(create_dim_cities_sql)
            logging.info(f"Table {schema_name}.dim_cities created or already exists.")
            connection.execute(create_dim_population_sql)
            logging.info(f"Table {schema_name}.dim_population created or already exists.")
            connection.execute(create_fact_weather_metrics_sql)
            logging.info(f"Table {schema_name}.fact_weather_metrics created or already exists.")

        logging.info(f"All tables in schema '{schema_name}' created or verified successfully.")

    except Exception as e:
        logging.error(f"Error creating tables in schema '{schema_name}': {e}")
        raise