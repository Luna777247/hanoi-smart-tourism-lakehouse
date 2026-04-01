# Hanoi Smart Tourism – Code Module

Domain: **Hanoi Smart Tourism – Attraction Performance Monitoring**  
Platform: **Lakehouse OSS** (vuong.ngo architecture)

---

## Cấu trúc thư mục (rút gọn)

```
.
├── apps/
│   ├── backend/             # FastAPI – portal API
│   └── frontend/            # Next.js portal
├── pipelines/
│   ├── airflow/             # DAG, plugin, Spark jobs (mount vào container Airflow/Spark)
│   ├── dbt/                 # Project dbt (models Gold/Silver, Iceberg)
│   └── notebooks/           # Jupyter – ví dụ Iceberg / streaming
├── infra/                   # Stack nền: Docker images & config (Trino, MinIO, Kafka, Spark, …)
├── scripts/http-examples/   # Log curl -v (debug OAuth/proxy)
├── data/seed/               # JSON mẫu / seed
├── docs/                    # Tài liệu dự án
├── docker-compose.yml
└── docker-compose.airflow-docker-exec.yml   # (tùy chọn) Docker socket cho DAG dùng docker exec
```

Chi tiết ngắn: `apps/README.md`, `pipelines/README.md`. DAG và Spark job chỉnh trong **`pipelines/airflow/`**.

### Container name cố định (docker exec)

Một số service dùng `container_name` để lệnh `docker exec …` ổn định:

| Service           | `container_name`   |
|-------------------|--------------------|
| Spark master      | `spark-master`     |
| Airflow scheduler | `airflow-scheduler`|
| Airflow worker    | `airflow-worker`   |

DAG `hanoi_full_pipeline` chạy **dbt** trực tiếp trong container Airflow (`source /opt/airflow/dbt_venv/bin/activate`, thư mục `DBT_PROJECT_DIR` thường là `/workspace/pipelines/dbt`). Bước **Spark** gọi `docker exec spark-master spark-submit …`.

Để bước Spark chạy được từ Celery worker, cần **Docker CLI** (đã thêm `docker.io` trong `pipelines/airflow/Dockerfile`) và mount socket:

```bash
docker compose -f docker-compose.yml -f docker-compose.airflow-docker-exec.yml up -d
```

Trên Linux, nếu gặp lỗi permission trên `/var/run/docker.sock`, cần đồng bộ nhóm `docker` giữa host và container (hoặc chỉ dùng DAG này trên môi trường dev có cấp quyền phù hợp). Trên Docker Desktop (Windows/macOS) thường chạy được với file merge trên.

---

## Luồng dữ liệu

```
Google Places API
      │
      ▼  [bronze_hanoi_attractions_dag – daily 02:00 ICT]
iceberg.bronze.hanoi_attractions
      │
      ▼  [silver_hanoi_attractions_dag – Dataset trigger]
iceberg.silver.dim_attraction
iceberg.silver.fact_attraction_snapshot
      │
      ▼  [gold_hanoi_dbt_dag – Dataset trigger]
iceberg.gold.gold_attractions_summary
iceberg.gold.gold_district_performance
iceberg.gold.gold_rating_trend
iceberg.gold.gold_overcrowding_alert
      │
      ▼
Apache Superset Dashboard
```

---

## Triển khai

### 1. Biến môi trường

Sao chép `.env.example` → `.env` và điền giá trị (đặc biệt `GOOGLE_PLACES_API_KEY`, `MINIO_ENDPOINT` cho DAG ingestion).

### 2. dbt – profiles

```bash
# Copy hoặc merge vào ~/.dbt/profiles.yml
cp pipelines/dbt/profiles.yml ~/.dbt/profiles.yml
```

Project dbt nằm tại **`pipelines/dbt/`**; Airflow dùng `DBT_PROJECT_DIR=/workspace/pipelines/dbt` (repo mount vào `/workspace`).

### 3. Iceberg schemas (Trino)

```sql
CREATE SCHEMA IF NOT EXISTS iceberg.bronze;
CREATE SCHEMA IF NOT EXISTS iceberg.silver;
CREATE SCHEMA IF NOT EXISTS iceberg.gold;
```

### 4. Bật DAG trong Airflow

```bash
docker exec -it airflow-scheduler airflow dags unpause bronze_hanoi_attractions
docker exec -it airflow-scheduler airflow dags unpause silver_hanoi_attractions
docker exec -it airflow-scheduler airflow dags unpause gold_hanoi_dbt
```

### 5. Trigger pipeline lần đầu

```bash
docker exec -it airflow-scheduler airflow dags trigger bronze_hanoi_attractions
```

### 6. dbt test

```bash
cd pipelines/dbt && dbt test --profiles-dir . --select hanoi_tourism
```

---

## Superset – Kết nối và Charts

| Chart | Dataset | Viz Type | Key columns |
|---|---|---|---|
| Bản đồ điểm du lịch | gold_attractions_summary | Deck.gl Scatter | lat, lng, current_rating |
| KPI – Tổng điểm | gold_attractions_summary | Big Number | COUNT(place_id) |
| KPI – Rating TB | gold_attractions_summary | Big Number | AVG(current_rating) |
| Top 10 điểm | gold_attractions_summary | Table | name, district, current_rating, current_reviews |
| Phân bố quận | gold_district_performance | Bar Chart | district, attraction_count |
| Xu hướng rating | gold_rating_trend | Line Chart | week_start, avg_rating (per district) |
| Cảnh báo Overcrowding | gold_overcrowding_alert | Table | name, risk_level, risk_note |

---

## dbt vars – Điều chỉnh ngưỡng Overcrowding

```bash
dbt run --select gold_overcrowding_alert \
  --vars '{"overcrowding_review_growth_threshold": 0.5, "overcrowding_min_rating": 4.5}'

dbt run --select gold_overcrowding_alert \
  --vars '{"overcrowding_review_growth_threshold": 0.2, "overcrowding_min_rating": 4.0, "overcrowding_min_reviews": 200}'
```

---

## Port mapping (Lakehouse platform – tham khảo)

| Service | Port(s) | Notes |
|--------|--------|-------|
| MinIO API | 9000 | S3-compatible |
| MinIO Console | 9001 | Web UI |
| Kafka | 9092 | Broker |
| Hive Metastore | 9083 | |
| Trino | 8090 | (có thể remap nếu trùng 8080) |
| Airflow API / UI | 8085 | Theo `docker-compose` hiện tại |
| Superset | 8089 | |
| PostgreSQL | 15432 | |
| Vault | 8200 | |

Chi tiết đầy đủ xem `docker-compose.yml` và `.env.example`.

---

## Airflow image – rebuild sau khi đổi dependencies

Khi sửa `pipelines/airflow/requirements.txt` hoặc `pipelines/airflow/Dockerfile`, build lại các service Airflow:

```bash
docker compose build airflow-apiserver airflow-scheduler airflow-worker airflow-triggerer airflow-dag-processor airflow-init
docker compose up -d
```
