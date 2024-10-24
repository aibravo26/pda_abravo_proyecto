FROM apache/airflow:2.10.2-python3.10

# Set the working directory
WORKDIR /app

# Copy pyproject.toml and poetry.lock first
COPY pyproject.toml poetry.lock /app/

# Install Poetry
RUN pip install poetry

# Switch to root user to install system dependencies
USER root

# Install necessary build tools and dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    libssl-dev \
    libffi-dev \
    python3-dev

# Install dependencies without dev dependencies
RUN poetry config virtualenvs.create false && poetry install --only main

# Switch back to airflow user for security best practices
USER airflow

# Copy the rest of the app
COPY . /app

# Expose Airflow's default port
EXPOSE 8080

# Set the entry point for the Airflow standalone server
CMD ["airflow", "standalone"]