"""
This module contains a function to check for and insert new cities from the staging table into the 
dim_cities table in the Redshift database. The process ensures that new cities are added only if 
they do not already exist in the dim_cities table.
"""

import os
import logging
from sqlalchemy import text

def check_new_cities_additions(engine):
    """Insert new capital_city values from staging_cities into dim_cities in a single query."""

    # Fetch schema from environment variable
    schema_name = os.getenv("REDSHIFT_SCHEMA")

    try:
        logging.info("Connecting to Redshift for schema '%s'.", schema_name)

        # SQL query to insert new cities from staging_cities into dim_cities
        insert_new_cities_sql = f"""
        INSERT INTO "{schema_name}".dim_cities (city_name, country, lat, lon)
        SELECT sc.capital_city, sc.country, sc.lat, sc.lon
        FROM "{schema_name}".staging_cities sc
        WHERE NOT EXISTS (
            SELECT 1 
            FROM "{schema_name}".dim_cities dc
            WHERE dc.city_name = sc.capital_city
        );
        """

        with engine.connect() as connection:
            logging.info("Executing query to insert new cities into %s.dim_cities.", schema_name)
            connection.execute(text(insert_new_cities_sql))

            logging.info("Inserted new cities into %s.dim_cities successfully.", schema_name)

    except Exception as error:
        logging.error("Error while inserting new cities into %s.dim_cities: %s", schema_name, error)
        raise
