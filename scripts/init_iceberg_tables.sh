
#!/bin/bash
# File: scripts/init_iceberg_tables.sh
# Chay sau khi Trino va MinIO da san sang
 
TRINO_HOST=${TRINO_HOST:-localhost}
TRINO_PORT=${TRINO_PORT:-8888}
 
run_sql() {
    docker compose exec trino trino \
        --server http://localhost:8080 \
        --execute "$1"
}
 
echo '==> Tao schemas'
run_sql 'CREATE SCHEMA IF NOT EXISTS iceberg.bronze'
run_sql 'CREATE SCHEMA IF NOT EXISTS iceberg.silver'
run_sql 'CREATE SCHEMA IF NOT EXISTS iceberg.gold'
 
echo '==> Tao Silver tables'
run_sql "
CREATE TABLE IF NOT EXISTS iceberg.silver.attractions_enriched (
    osm_id              VARCHAR,
    name                VARCHAR,
    address             VARCHAR,
    latitude            DOUBLE,
    longitude           DOUBLE,
    rating              DOUBLE,
    review_count        INTEGER,
    website             VARCHAR,
    phone               VARCHAR,
    poi_types           ARRAY(VARCHAR),
    run_id              VARCHAR,
    snapshot_date       VARCHAR,
    source_ingested_at  TIMESTAMP(6),
    silver_processed_at TIMESTAMP(6),
    business_key        VARCHAR
)
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['day(silver_processed_at)'],
    location     = 's3a://tourism-silver/attractions_enriched/'
)"
 
echo '==> Tao Gold tables'
run_sql "
CREATE TABLE IF NOT EXISTS iceberg.gold.mart_district_stats (
    report_date         DATE,
    district            VARCHAR,
    total_attractions   BIGINT,
    avg_rating          DOUBLE,
    total_reviews       BIGINT,
    max_reviews         INTEGER
)
WITH (
    format       = 'PARQUET', 
    partitioning = ARRAY['report_date'],
    location     = 's3a://tourism-gold/mart_district_stats/'
)"

run_sql "
CREATE TABLE IF NOT EXISTS iceberg.gold.mart_tourism_heatmap (
    report_date      DATE,
    name             VARCHAR,
    latitude         DOUBLE,
    longitude        DOUBLE,
    rating           DOUBLE,
    review_count     INTEGER,
    district         VARCHAR,
    popularity_score DOUBLE
)
WITH (
    format       = 'PARQUET', 
    partitioning = ARRAY['report_date'],
    location     = 's3a://tourism-gold/mart_tourism_heatmap/'
)"
 
echo '==> Iceberg tables created successfully!'
