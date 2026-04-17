#!/bin/bash
# File: scripts/start_docker_services.sh
# Script để khởi động các services cần thiết cho data ingestion

echo "🚀 Starting Docker services for Hanoi Tourism Lakehouse..."

# Dừng services cũ nếu đang chạy
docker compose --profile "*" down

# Khởi động core services + ingestion stack
docker compose up -d \
    postgres redis minio vault \
    spark-master spark-worker \
    airflow-init airflow-api-server airflow-scheduler \
    trino superset

echo "⏳ Waiting for services to be ready..."
sleep 30

# Check health
echo "🔍 Checking service health..."
docker ps --filter "name=lakehouse-" --format "table {{.Names}}\t{{.Status}}"

echo "✅ Services started successfully!"
echo ""
echo "📊 Access URLs:"
echo "  Airflow UI: http://localhost:8080"
echo "  MinIO Console: http://localhost:9001"
echo "  Superset: http://localhost:8088"
echo "  Trino: http://localhost:8888"
echo ""
echo "💡 To trigger data ingestion, go to Airflow UI and run DAGs:"
echo "  - bronze_ingest_osm_google_enriched (daily 02:00)"
echo "  - silver_transform_enriched_data (daily 05:00)"
echo "  - gold_transform_tourism_marts (daily 06:00)"
echo "  - master_pipeline_hanoi_tourism (manual orchestration)"
