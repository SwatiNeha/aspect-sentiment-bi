FROM apache/airflow:2.9.0

# Install system dependencies (needed for spacy, torch, etc.)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc g++ \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy and install Python deps
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt