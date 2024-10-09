## Project Overview

Here’s a brief overview of the project: it involves a script that extracts data from two APIs (weather and population) for a set of cities defined in an input file. The data is then stored in a staging area within Redshift, with the aim of creating a star schema.

## Data Flow

The models in the staging area (`staging_cities`, `staging_api_population_data`, `staging_api_weather_data`) are overwritten each time the DAG is executed. These models then feed the star schema.

![image](https://github.com/user-attachments/assets/6a0faa58-a30d-4d82-924d-af0798457bdd)

## Star Schema Structure

- **dim_cities**: A dimension table for cities, where new cities are added as they appear in `staging_cities`.
- **dim_population**: An SCD2-type population dimension that stores the population of cities and their evolution over time. It includes fields such as `effective_date`, `expiration_date`, and `is_current`.
- **fact_weather_metrics**: An incremental fact table that stores weather metrics. This model adds new data on each DAG execution. Being an incremental model, it uses the `date` and `execution timestamp` as keys to ensure new information is stored with every run.

![DB ERD](https://github.com/user-attachments/assets/4875a92c-9fd7-4e94-b892-4a9a8dc23504)
