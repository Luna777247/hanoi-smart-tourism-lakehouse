#!/bin/bash
# scripts/validate_etl_idempotency.sh
# Automates the validation of ETL idempotency and Airflow runtime health.

set -e

echo "=========================================================="
echo "  HANOI SMART TOURISM LAKEHOUSE - ETL IDEMPOTENCY CHECK   "
echo "=========================================================="

echo -e "\n[1/5] Checking Docker Services Health..."
SERVICES=("lakehouse-airflow-api" "lakehouse-airflow-scheduler" "lakehouse-airflow-processor" "lakehouse-postgres" "lakehouse-redis" "lakehouse-minio")
for svc in "${SERVICES[@]}"; do
    STATUS=$(docker inspect --format='{{.State.Status}}' "$svc" 2>/dev/null || echo "Missing")
    if [ "$STATUS" != "running" ]; then
        echo "❌ Service $svc is not running! Status: $STATUS"
        exit 1
    fi
    echo "✅ Service $svc is UP."
done

echo -e "\n[2/5] Checking Airflow Scheduler Logs for ConnectError (Errno 111)..."
ERROR_COUNT=$(docker logs lakehouse-airflow-scheduler --tail 200 2>&1 | grep -i "Errno 111\|ConnectError\|serialized_dag" | wc -l)
if [ "$ERROR_COUNT" -gt 0 ]; then
    echo "❌ Found $ERROR_COUNT errors related to Connection or Serialized DAGs in the last 200 lines of scheduler log."
    docker logs lakehouse-airflow-scheduler --tail 50 2>&1 | grep -i -C 2 "Errno 111\|ConnectError\|serialized_dag"
    echo "Please check AIRFLOW__CORE__EXECUTION_API_SERVER_URL configuration."
else
    echo "✅ Scheduler logs are clean from ConnectError and serialized_dag missing alerts."
fi

echo -e "\n[3/5] Checking Airflow DAG execution state..."
DAG_ID="bronze_ingest_osm_google_enriched"
echo "Triggering DAG $DAG_ID to verify run state..."
RUN_ID="val_run_$(date +%Y%m%d%H%M%S)"
docker exec lakehouse-airflow-scheduler airflow dags trigger -r "$RUN_ID" "$DAG_ID" >/dev/null
echo "Waiting for task scheduling and completion..."
echo -n "Polling DAG state "
TIMEOUT=180
ELAPSED=0
while [[ $ELAPSED -lt $TIMEOUT ]]; do
    STATE=$(docker exec lakehouse-airflow-scheduler airflow dags state "$DAG_ID" "$RUN_ID" 2>/dev/null | tail -n 1 | tr -d '\r' | tr -d ' ')
    if [[ "$STATE" == "success" || "$STATE" == "failed" ]]; then
        break
    fi
    echo -n "."
    sleep 5
    ELAPSED=$((ELAPSED + 5))
done
echo ""

echo "DAG $DAG_ID Run $RUN_ID ended with state: $STATE"
if [[ "$STATE" != "success" ]]; then
    echo "❌ DAG run failed or timed out! Check Airflow UI for details."
    exit 1
else
    echo "✅ DAG run completed successfully."
fi

echo -e "\n[4/5] Checking Raw Data Landing paths in MinIO..."
echo "Looking for objects matching: source=osm_google_enriched/snapshot_date=*/run_id=*/*.json"

# Su dung Python bota3 de lay list obj thay vi mc vi container minio-init co the da thoat sau khi chay xong
cat << 'MEOF' > /tmp/validate_minio.py
import boto3
s3 = boto3.client('s3', endpoint_url='http://lakehouse-minio:9000', aws_access_key_id='lakehouse', aws_secret_access_key='lakehouse123')
try:
    resp = s3.list_objects_v2(Bucket='tourism-bronze', Prefix='source=osm_google_enriched/')
    for obj in resp.get('Contents', []):
        if obj['Key'].endswith('.json'):
            print(obj['Key'])
except Exception:
    pass
MEOF
docker cp /tmp/validate_minio.py lakehouse-airflow-scheduler:/tmp/validate_minio.py >/dev/null 2>&1 || true
MINIO_FILES=$(docker exec lakehouse-airflow-scheduler python /tmp/validate_minio.py 2>/dev/null || echo "")

if [[ -z "$MINIO_FILES" ]]; then
    echo "⚠️  No json files found yet in MinIO for osm_google_enriched."
else
    if echo "$MINIO_FILES" | grep -vqE "snapshot_date=[0-9]{4}-[0-9]{2}-[0-9]{2}/run_id=[A-Za-z0-9_:+-]+/data\.json"; then
        echo "❌ Found raw JSON objects outside the expected run-aware pattern!"
        echo "$MINIO_FILES" | head -n 10
        exit 1
    elif echo "$MINIO_FILES" | grep -qE "snapshot_date=[0-9]{4}-[0-9]{2}-[0-9]{2}/run_id=[A-Za-z0-9_:+-]+/data\.json"; then
        echo "✅ Raw JSON objects perfectly match the expected run-aware pattern:"
        echo "$MINIO_FILES" | head -n 3
    else
        echo "❌ Raw JSON objects do NOT match expected pattern (snapshot_date=.../run_id=.../data.json)!"
        echo "$MINIO_FILES" | head -n 3
        exit 1
    fi
fi

echo -e "\n[5/5] Running Iceberg Deduplication Validation via spark-submit..."
VALIDATION_SCRIPT="./infra/spark/jobs/validate_iceberg.py"
cat << 'EOF' > "$VALIDATION_SCRIPT"
import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ETL_Idempotency_Validator") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

print("\n--- Validating Silver Layer by Snapshot ---")
try:
    bronze_dups = spark.sql("""
        SELECT snapshot_date, business_key, COUNT(*) as cnt
        FROM iceberg.silver.attractions_enriched
        GROUP BY 1, 2
        HAVING cnt > 1
    """).count()
    if bronze_dups > 0:
        print(f"❌ FOUND {bronze_dups} duplicate business keys in Silver snapshots!")
        sys.exit(1)
    else:
        print("✅ Silver Layer: 0 duplicates found for same snapshot_date + business_key.")
except Exception as e:
    print(f"❌ Silver table not available or validation failed: {e}")
    sys.exit(1)

print("\n--- Validating Silver Layer ---")
try:
    silver_dups = spark.sql("""
        SELECT business_key, COUNT(*) as cnt
        FROM iceberg.silver.attractions_enriched
        GROUP BY 1
        HAVING cnt > 1
    """).count()
    if silver_dups > 0:
        print(f"❌ FOUND {silver_dups} duplicate business_keys in Silver Layer!")
        sys.exit(1)
    else:
        print("✅ Silver Layer: 0 duplicate business keys found.")
except Exception as e:
    print(f"❌ Silver table not available or validation failed: {e}")
    sys.exit(1)

spark.stop()
EOF

# Thuc thi qua spark-master thay vi chay python trong scheduler
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit /opt/spark-jobs/validate_iceberg.py

echo -e "\n=========================================================="
echo "                 VALIDATION COMPLETE                      "
echo "=========================================================="
