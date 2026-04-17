# Hanoi Smart Tourism Lakehouse: Pipeline Stabilization Report

## 1. Overview

The data pipeline for Hanoi Smart Tourism has been stabilized and validated from Bronze to Gold layer. All catalog synchronization issues between Spark (ETL) and Trino (Query Engine) have been resolved by implementing a unified **JDBC Iceberg Catalog**.

## 2. Key Changes & Resolutions

### A. Catalog Synchronization (JDBC Migration)

- **Problem:** Spark was using a Hadoop-based catalog (metadata on S3) while Trino was using a JDBC-based catalog (metadata in Postgres). This made tables created by Spark invisible to Trino.
- **Solution:** Migrated all Spark jobs and Airflow DAGs to use the **JDBC Catalog** connector.
- **Configuration:** Both systems now point to the `lakehouse_meta` database in Postgres using the catalog name `iceberg`.

### B. Silver Layer Data Processing

- **Fix:** Handled `AnalysisException` caused by missing fields (`types`, `website`, `phone`) in the source JSON by implementing dynamic schema selection in `silver_process_enriched_data.py`.
- **Result:** **115 records** successfully enriched and persisted in `iceberg.silver.attractions_enriched`.

### C. Gold Layer Transformation

- **Fix:** Updated `gold_process_tourism_marts.py` to correctly reference the new Iceberg table locations.
- **Result:** Successfully generated two high-value marts:
  - `gold.mart_district_stats`: Statistical view per district.
  - `gold.mart_tourism_heatmap`: Geospatial data for tourist density.

### D. Airflow DAG Optimization

- Updated `silver_transform_enriched_data.py` and `gold_transform_tourism_marts.py` with the correct Spark configurations, ensuring that scheduled runs will maintain metadata consistency.

## 3. Data Validation (Trino Results)

| Layer | Table | Record Count | Status |
|---|---|---|---|
| **Silver** | `attractions_enriched` | 115 | ✅ Validated |
| **Gold** | `mart_district_stats` | 16 | ✅ Validated |
| **Gold** | `mart_tourism_heatmap` | 115 | ✅ Validated |

## 4. Maintenance Notes

- **JDBC Credentials:** Stored in Airflow DAGs (User: `lakehouse_admin`, Pass: `ChangeMe_Strong123!`).
- **Warehouse Location:** S3A paths are preserved for data storage (`s3a://tourism-silver/warehouse`).
- **Catalog Name:** Use `iceberg` as the root catalog in all SQL queries (e.g., `SELECT * FROM iceberg.silver.attractions`).

---
*Report generated on 2026-04-16*
