# `pipelines/` — luồng dữ liệu & biến đổi

| Thư mục   | Vai trò |
|----------|---------|
| `airflow/` | DAG, plugin Airflow, Spark jobs mount vào container (`dags/`, `processing/spark/jobs/`) |
| `dbt/`    | Project dbt (Bronze/Silver/Gold models, Iceberg) |
| `notebooks/` | Jupyter — ví dụ Iceberg, streaming |

`infra/` ở gốc repo giữ **stack nền** (Trino, MinIO, Kafka, …); code pipeline nằm tại đây để tách biệt với ứng dụng (`apps/`) và dễ mở rộng domain mới (thêm thư mục con trong `dbt/models`, DAG mới trong `airflow/dags`).
