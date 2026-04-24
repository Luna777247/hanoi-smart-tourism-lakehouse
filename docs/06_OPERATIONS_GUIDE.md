# 06. Hướng dẫn Vận hành (Operations Guide)

## 6.1. Khởi động hệ thống

Sử dụng script quản trị `manage.ps1` (PowerShell):

1. **Foundation:** `.\manage.ps1 up-foundation` (Storage & DB)
2. **Orchestration:** `.\manage.ps1 up-orchestration` (Airflow)
3. **ETL Engine:** `.\manage.ps1 up-etl` (Spark & Trino)

## 6.2. Chạy quy trình Dữ liệu

1. **Nhập liệu (Bronze):** Trigger DAG `bronze_ingest_osm_google_enriched`.
2. **Làm sạch (Silver):** Trigger DAG `silver_transform_enriched_data`.
3. **Hợp nhất (Gold):** Trigger DAG `gold_transform_tourism_marts` (Chạy bằng dbt).

## 6.3. Khắc phục lỗi (Troubleshooting)

* **Lỗi Quota API:** Kiwi/Google Maps API trả về 429. *Giải pháp:* Chờ chu kỳ 24h hoặc đổi API Key trong tệp `.env`.
* **Trino không khởi động:** Thiếu RAM. *Giải pháp:* Tăng RAM của Docker lên tối thiểu 8GB.
* **dbt Compile Error:** Sai syntax trong models. *Giải pháp:* Kiểm tra log tại `/opt/airflow/dbt_project/logs/dbt.log`.
* **Lỗi OpenMetadata "Index all not found":** Mất index trong Elasticsearch. *Giải pháp:* Chạy `docker exec lakehouse-openmetadata /opt/openmetadata/bootstrap/openmetadata-ops.sh reindex --force`.
