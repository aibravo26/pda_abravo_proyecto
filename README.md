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

![ERD - Logistics_ops  - ERD](https://github.com/user-attachments/assets/930fd09e-6328-49bf-9432-0fa1d3119506)

## Steps to Run the Project in Docker using Docker Compose

### 1. Clone the repository
Open your terminal and clone the repository by running:
```bash
git clone https://github.com/aibravo26/pda_abravo_proyecto.git
cd pda_abravo_proyecto
```

### 2. Set up environment variables
Ensure you have a `.env` file that contains the necessary sensitive information, such as API keys and Redshift credentials.

```
# .env file

AIRFLOW_UID=
OPENWEATHERMAP_API_KEY=
GEONAMES_API_KEY=
REDSHIFT_HOST=
REDSHIFT_PORT=
REDSHIFT_DBNAME=
REDSHIFT_USER=
REDSHIFT_PASSWORD=
REDSHIFT_SCHEMA=
AIRFLOW_HOME=
```

For security and privacy matters, these values are not shared in this repository. Please contact me at **bravo.ag26@gmail.com** to obtain the necessary environment variable values.

### 3. Build and run the services using Docker Compose
You can start the whole setup, including linked services like PostgreSQL or Redshift, by running:
```bash
docker-compose up -d --build
```

Docker Compose will automatically handle building the images and running the services defined in the `docker-compose.yml` file.

### 4. Access the Airflow web interface
Once the services are running, you can access the Airflow web interface by opening your browser and navigating to:
```
http://localhost:8080
```

### 5. Access the running container
If needed, you can access the container's shell to inspect it or run additional commands:
```bash
docker exec -it <container_id> /bin/bash
```
Replace `<container_id>` with the actual container ID, which you can find by running:
```bash
docker ps
```

### 6. Check logs and output
You can check the container's logs to see the output from the project:
```bash
docker logs <container_id>
```

### 7. Stopping the services
When you're done testing, you can stop the running services with:
```bash
docker-compose down
```

### 8. Rebuilding and restarting
If you make changes and need to rebuild the services, you can use:
```bash
docker-compose up --build
```
