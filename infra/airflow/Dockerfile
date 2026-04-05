# Builder stage for installing Java and Docker
FROM debian:bookworm-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless curl tar docker.io && \
    rm -rf /var/lib/apt/lists/*

# Final stage
FROM apache/airflow:3.0.6-python3.12

# Copy Java and Docker from builder
COPY --from=builder /usr/lib/jvm/java-17-openjdk-amd64 /usr/lib/jvm/java-17-openjdk-amd64
COPY --from=builder /usr/bin/docker /usr/bin/docker

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Copy và cài đặt requirements cho Airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# COPY ./dags/dag_sync_manager.py /opt/airflow/dags
USER airflow

# ─── Cài đặt dbt Virtual Environment ───────────
USER airflow
RUN python3 -m venv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir \
        "dbt-spark[PyHive]" \
        "dbt-trino" \
        "astronomer-cosmos" \
        "psycopg2-binary" \
        "asyncpg"
# ──────────────────────────────────────────────

# RUN pip install 'acryl-datahub-airflow-plugin>=1.1.0.4'