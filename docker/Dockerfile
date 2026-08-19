
# Dockerfile for OULAD Student Risk Segmentation Pipeline
# Georgia State University Capstone – Module 3
# Student: Gloria Ayesiga
 
FROM python:3.11-slim
 
LABEL maintainer="Gloria Ayesiga"
LABEL project="GSU Student Risk Segmentation – Module 3"
LABEL description="Reproducible container for the OULAD ETL + validation pipeline"
 
# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*
 
WORKDIR /app
 
# Copy dependency list first (better layer caching)
COPY requirements.txt .
 
RUN pip install --no-cache-dir -r requirements.txt
 
# Copy pipeline code
COPY flows/ ./flows/
COPY great_expectations/ ./great_expectations/
COPY scripts/ ./scripts/
 
# Create expected data and log directories
RUN mkdir -p data/raw data/processed logs
 
# Default command runs the Prefect flow
# Mount your local data/raw folder at runtime:
#   docker run --rm -v $(pwd)/data:/app/data gsu-oulad-pipeline
CMD ["python", "-m", "flows.oulad_etl_flow"]