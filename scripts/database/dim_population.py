"""
This module provides functionality to update the dim_population table in Redshift using 
Slowly Changing Dimension (SCD) Type 2 logic. The update handles new population changes 
and maintains historical data.
"""

import os
import logging

def check_population_updates(engine):
    """Update dim_population table with SCD Type 2 logic, handling new population changes."""

    # Fetch schema from environment variable
    schema_name = f'"{os.getenv("REDSHIFT_SCHEMA")}"'

    try:
        logging.info(
            "Starting SCD Type 2 update for dim_population in schema '%s'.",
            schema_name
        )

        update_population_sql = f"""
        BEGIN TRANSACTION;

        -- Step 0: Create a temporary table to store the current timestamp
        CREATE TEMP TABLE temp_dates AS 
        SELECT CURRENT_TIMESTAMP AS current_date;

        -- Step 1: Insert new records for population changes
        INSERT INTO {schema_name}.dim_population (
            city_id, population, effective_date, expiration_date, is_current
        )
        SELECT
            d.id,
            s.population,
            temp_dates.current_date AS effective_date,
            '9999-12-31' AS expiration_date,
            TRUE AS is_current
        FROM {schema_name}.staging_api_population_data s
        LEFT JOIN {schema_name}.dim_cities d ON s.capital_city = d.city_name
        LEFT JOIN {schema_name}.dim_population dp
            ON d.id = dp.city_id
            AND dp.is_current = TRUE
        CROSS JOIN temp_dates
        WHERE dp.city_id IS NULL
        OR dp.population != s.population;

        -- Step 2: Expire old records where population has changed
        UPDATE {schema_name}.dim_population
        SET expiration_date = temp_dates.current_date, 
            is_current = FALSE
        FROM {schema_name}.staging_api_population_data s
        LEFT JOIN {schema_name}.dim_cities d ON s.capital_city = d.city_name
        CROSS JOIN temp_dates
        WHERE {schema_name}.dim_population.city_id = d.id
        AND {schema_name}.dim_population.is_current = TRUE
        AND {schema_name}.dim_population.population != s.population;

        -- Step 3: Drop the temporary table
        DROP TABLE IF EXISTS temp_dates;

        COMMIT;
        """

        with engine.connect() as connection:
            logging.info(
                "Executing population update SQL in schema '%s'.",
                schema_name
            )
            connection.execute(update_population_sql)
        logging.info(
            "dim_population successfully updated in schema '%s'.",
            schema_name
        )
    except Exception as error:
        logging.error("Error updating dim_population: %s", error)
        raise
