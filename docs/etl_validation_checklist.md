# ETL Idempotency & Validation Checklist

This document outlines the strict criteria and checks required to ensure the ETL pipelines are flawlessly idempotent and robust within the Airflow 3 Lakehouse architecture.

## 1. Airflow Runtime & Infrastructure Stability

- [ ] **Services are Up**: `lakehouse-airflow-api`, `lakehouse-airflow-scheduler`, `lakehouse-airflow-processor`, `postgres`, `redis`, `minio` are all `Up` and `Healthy` in docker-compose.
- [ ] **No Execution API Connection Refusals**: Verify `docker compose logs airflow-scheduler --tail=200` has NO `Errno 111` or `ConnectError`. The `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` config has safely resolved this.
- [ ] **DAG Processor Syncs Correctly**: DAGs are parsed and appear in `airflow dags list` or Web UI without missing `serialized_dag` warnings.
- [ ] **API Server is Stable**: `airflow-api-server` logs do not restart constantly and handle executions without dropping SDK sessions.

## 2. DAG Execution Reliability

- [ ] **Successful Triggers**: Command `airflow dags trigger bronze_ingest_osm_google_enriched` transitions appropriately from `queued` -> `running` -> `success`.
- [ ] **Zero Blocking in Task Supervisors**: Task `enrich_attractions_osm_google` actually executes Python operations instead of getting stuck in `queued` loop timeouts.
- [ ] **Fault Isolation**: Any task `FAILED` state must be distinctly traceable to external data/API errors in task logs, never core internal orchestrator connectivity issues.

## 3. MinIO Landing Object Completeness

- [ ] **Run-Aware Partitioning**: Confirm that MinIO (`tourism-bronze` bucket) stores data strictly using the hierarchical structure:
  - `source=osm_google_enriched/snapshot_date=YYYY-MM-DD/run_id=UUID/data.json`
- [ ] **Immutable Raw Data Schema**: The actual raw json file payload remains pure. All metadata (`snapshot_date`, `run_id`) is inferred dynamically from the object folder URI paths in Spark, leaving the base API output flat and clean.

## 4. Idempotency Flow & Deduplication Guarantees

- [ ] **Idempotent Day Re-runs (Skip)**: Triggering the DAG again on the same day with `force=False` strictly Skips (No-op), safeguarding resources.
- [ ] **Auditable Forced Re-runs**: Triggering the DAG on the same day with `force=True` generates a **new `run_id` prefix** in MinIO but **preserves** the previous run.
- [ ] **Bronze Layer `MERGE` Mechanics**: Iceberg ingestor must run a `MERGE INTO`, assuring `row_count` does NOT inflate if `snapshot_date` + `record_key` duplicate and hashes match. Updates only apply if `record_hash` differs.
- [ ] **Silver Layer Windowing**: Reruns of Silver process do not append dupes. They use `business_key` partition Windows filtering explicitly for `row_number() over (... order by source_ingested_at desc, run_id desc) = 1` resolving exactly 1 latest truth per entity.

## 5. Quick Verification Queries

You can run the following test queries via Trino or Spark SQL to confirm state.
Or, execute `bash scripts/validate_etl_idempotency.sh` to fully automate the checks:

### 5.1 Silver Consistency

```sql
SELECT snapshot_date, COUNT(*) 
FROM iceberg.silver.attractions_enriched
GROUP BY 1
ORDER BY 1 DESC;
```

### 5.2 Assert Zero Duplications in Silver by Snapshot

```sql
SELECT snapshot_date, business_key, COUNT(*)
FROM iceberg.silver.attractions_enriched
GROUP BY 1,2
HAVING COUNT(*) > 1;
```

### 5.3 Assert Zero Duplications in Silver

```sql
SELECT business_key, COUNT(*)
FROM iceberg.silver.attractions_enriched
GROUP BY 1
HAVING COUNT(*) > 1;
```

## Maintenance Notes

Continuous integration validation should trigger `scripts/validate_etl_idempotency.sh` on every schedule run or major commit fixing core Python jobs.
