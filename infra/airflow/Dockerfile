# File: infra/airflow/Dockerfile
FROM apache/airflow:3.0.0

USER root
# Install git, uv, and Java (required for Spark)
RUN apt-get update && apt-get install -y git curl openjdk-17-jre-headless && \
    curl -LsSf https://astral.sh/uv/install.sh | sh && \
    ln -s /root/.cargo/bin/uv /usr/local/bin/uv && \
    rm -rf /var/lib/apt/lists/*

USER airflow
COPY --chown=airflow:0 infra/airflow/requirements.txt /requirements.txt
# Use uv for faster and smarter dependency resolution
RUN uv pip install --no-cache-dir -r /requirements.txt

USER airflow

# Bake application logic into the image
COPY --chown=airflow:0 ./infra/airflow/dags /opt/airflow/dags
COPY --chown=airflow:0 ./dbt_project /opt/airflow/dbt_project
COPY --chown=airflow:0 ./infra/airflow/plugins /opt/airflow/plugins

# Pre-parse dbt to ensure safety and performance
WORKDIR /opt/airflow/dbt_project
RUN dbt parse --profiles-dir . || echo "Warning: dbt parse failed, check your profiles.yml"

WORKDIR /opt/airflow
