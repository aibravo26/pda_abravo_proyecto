# Weather & Population Data Orchestrator

## Project Overview

Here’s a brief overview of the project: it involves a script that extracts data from two APIs (weather and population) for a set of cities defined in an input file. The data is then stored in a staging area within Redshift, with the aim of creating a star schema.
* [Open Weather Map](https://openweathermap.org/)
* [GeoNames](https://www.geonames.org/)

## Key Features
* **Python-based Project:** This project is written using Python, leveraging its robust libraries and tools for data extraction, transformation, and loading (ETL).
* **Dependency Management with Poetry:** All packages and dependencies are managed with Poetry, ensuring consistent environments and easy setup.
* **Quality Assurance with Unit Tests:** The project includes unit tests to ensure code quality and functionality, improving the reliability of data pipelines.
* **Linting for Best Code Practices:** Linting tools such as Pylint and Flake8 are integrated into the project to enforce coding standards, ensure readability, and prevent common programming errors, promoting maintainable and clean code.
* **Workflow Orchestration with Apache Airflow:** Apache Airflow is used to orchestrate the ETL processes, managing the scheduling and execution of data workflows.
* **Redshift as Data Warehouse:** Data is stored in Amazon Redshift, a highly scalable cloud-based data warehouse, enabling fast and efficient querying of large datasets.
* **Containerization with Docker:** The entire project is containerized with Docker, making it easy to deploy and run the application in any environment.

## Data Flow

The models in the staging area (`staging_cities`, `staging_api_population_data`, `staging_api_weather_data`) are overwritten each time the DAG is executed. These models then feed the star schema.

![image](https://github.com/user-attachments/assets/6a0faa58-a30d-4d82-924d-af0798457bdd)

## Data Model Overview

- **dim_cities**:  
  A dimension table for storing city information. New cities are added automatically from `staging_cities` as they appear.

- **dim_population**:  
  A slowly changing dimension (SCD Type 2) that tracks city populations and how they evolve over time. It includes the following fields:
  - `effective_date`: The date when the population data becomes valid.
  - `expiration_date`: The date when the population data is no longer valid.
  - `is_current`: A flag indicating if the record is the latest version of the data.

- **fact_weather_metrics**:  
  An incremental fact table that stores weather data. Each time the DAG runs, it adds new data. The model uses `date` and `execution timestamp` as keys to ensure that only new weather information is added on every run.

![DB ERD](https://github.com/user-attachments/assets/5b3cbc81-6449-4c31-8e74-5757c96ad502)
