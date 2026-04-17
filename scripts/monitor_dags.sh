#!/bin/bash
# File: scripts/monitor_dags.sh
# Script để monitor trạng thái DAGs trong Airflow

echo "🔍 Monitoring Airflow DAGs status..."

# Lấy danh sách DAGs và trạng thái
docker exec lakehouse-airflow-worker airflow dags list --output table | grep -E "(osm_google_enriched|silver_transform_enriched_data|gold_transform_tourism_marts|master_pipeline)"

echo ""
echo "📋 Recent DAG runs (last 24h):"
docker exec lakehouse-airflow-worker airflow dags unpause bronze_ingest_osm_google_enriched 2>/dev/null
docker exec lakehouse-airflow-worker airflow dags unpause silver_transform_enriched_data 2>/dev/null
docker exec lakehouse-airflow-worker airflow dags unpause gold_transform_tourism_marts 2>/dev/null
docker exec lakehouse-airflow-worker airflow dags unpause master_pipeline_hanoi_tourism 2>/dev/null

# Check failed DAGs trong 24h qua
echo ""
echo "❌ Failed DAG runs in last 24h:"
FAILED_RUNS=$(docker exec lakehouse-airflow-worker airflow dags state --output json | jq -r '.[] | select(.state == "failed") | select(.execution_date > (now - 86400 | tostring + "Z" | fromdate)) | "\(.dag_id) - \(.execution_date) - \(.state)"' 2>/dev/null)

if [ -z "$FAILED_RUNS" ]; then
    echo "✅ No failed DAG runs in the last 24 hours"
else
    echo "$FAILED_RUNS"
fi

echo ""
echo "💡 Access Airflow UI: http://localhost:8080"
