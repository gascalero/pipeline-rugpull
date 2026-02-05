# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /mlflow

# Install uv (mucho más rápido que pip)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install system dependencies for MLFlow + PostgreSQL
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy requirements first (mejor para cache de Docker)
COPY requirements.txt .

# Install MLFlow and dependencies using uv
RUN uv pip install --system --no-cache -r requirements.txt

# Copy the rest of the files
COPY . /mlflow

# Expose the port that MLFlow uses
EXPOSE 5000

# Command to run the MLFlow server
# Las variables de entorno se pasan desde docker-compose.yml
CMD ["mlflow", "server", \
    "--backend-store-uri", "${MLFLOW_BACKEND_STORE_URI}", \
    "--default-artifact-root", "${MLFLOW_DEFAULT_ARTIFACT_ROOT}", \
    "--host", "0.0.0.0", \
    "--port", "5000"]