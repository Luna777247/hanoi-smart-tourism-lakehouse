# `infra/` — nền tảng vận hành (Docker / config)

Thư mục này chứa **context build**, file cấu hình và script cho từng dịch vụ nền: MinIO, Trino, Hive Metastore, Kafka, Spark image, Superset, v.v.

Code **DAG Airflow**, **Spark jobs** và **dbt** nằm trong [`../pipelines/`](../pipelines/README.md), không trộn vào đây — giúp tách “stack hạ tầng” khỏi “logic pipeline” khi mở rộng dự án.
