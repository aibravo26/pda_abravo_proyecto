import os
import logging
from sqlalchemy import text
from apis_etl.utils import connect_to_redshift

def check_new_citites_additions(engine):
    """Insert new capital_city values from staging_cities into dim_cities in a single query."""
    
    # Fetch schema from environment variable
    schema_name = f'"{os.getenv("REDSHIFT_SCHEMA")}"' 

    try:
        logging.info(f"Connecting to Redshift for schema '{schema_name}'.")

        # SQL query to insert new cities from staging_cities into dim_cities
        insert_new_cities_sql = f"""
        INSERT INTO {schema_name}.dim_cities (city_name, country, lat, lon)
        SELECT sc.capital_city, sc.country, sc.lat, sc.lon
        FROM {schema_name}.staging_cities sc
        WHERE NOT EXISTS (
            SELECT 1 
            FROM {schema_name}.dim_cities dc
            WHERE dc.city_name = sc.capital_city
        );
        """

        with engine.connect() as connection:
            logging.info(f"Executing query to insert new cities into {schema_name}.dim_cities.")
            result = connection.execute(text(insert_new_cities_sql))

            logging.info(f"Inserted new cities into {schema_name}.dim_cities successfully.")

    except Exception as e:
        logging.error(f"Error while inserting new cities into {schema_name}.dim_cities: {e}")
        raise