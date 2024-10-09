import os
import logging
from sqlalchemy import text

def check_population_updates(engine):
    """Update dim_population table with SCD Type 2 logic, handling new population changes."""

    # Fetch schema from environment variable
    schema_name = f'"{os.getenv("REDSHIFT_SCHEMA")}"' 

    try:
        logging.info(f"Starting SCD Type 2 update for dim_population in schema '{schema_name}'.")

        update_population_sql = f"""
        BEGIN TRANSACTION;

        -- Step 1: Insert new records for population changes
        INSERT INTO {schema_name}.dim_population (
            city_id, population, effective_date, expiration_date, is_current
        )
        SELECT
            d.id,
            s.population,
            CURRENT_TIMESTAMP AS effective_date,
            '9999-12-31' AS expiration_date,
            TRUE AS is_current
        FROM {schema_name}.staging_api_population_data s
        LEFT JOIN {schema_name}.dim_cities d ON s.capital_city = d.city_name
        LEFT JOIN {schema_name}.dim_population dp
            ON d.id = dp.city_id
            AND dp.is_current = TRUE
        WHERE dp.city_id IS NULL
        OR dp.population != s.population;

        -- Step 2: Expire old records where population has changed
        UPDATE {schema_name}.dim_population
        SET expiration_date = CURRENT_TIMESTAMP, 
            is_current = FALSE
        FROM {schema_name}.staging_api_population_data s
        LEFT JOIN {schema_name}.dim_cities d ON s.capital_city = d.city_name
        WHERE {schema_name}.dim_population.city_id = d.id
        AND {schema_name}.dim_population.is_current = TRUE
        AND {schema_name}.dim_population.population != s.population;

        COMMIT;
        """

        with engine.connect() as connection:
            logging.info(f"Executing population update SQL in schema '{schema_name}'.")
            connection.execute(update_population_sql)
        logging.info(f"dim_population successfully updated in schema '{schema_name}'.")
    except Exception as e:
        logging.error(f"Error updating dim_population: {e}")
        raise