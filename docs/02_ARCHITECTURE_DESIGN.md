# 02. Kiến trúc Hệ thống (Architecture Design)

## 2.1. Mô hình Medallion Lakehouse

Hệ thống được tổ chức theo kiến trúc Medallion trên nền tảng **Apache Iceberg**, đảm bảo tính toàn vẹn (ACID) và hiệu năng truy vấn cao.

* **Tầng Bronze (Raw):** Lưu trữ dữ liệu thô từ OSM và Google JSON. Phân vùng theo `source` và `snapshot_date`.
* **Tầng Silver (Cleansed & Unified):** Dữ liệu được làm sạch, hợp nhất (Deduplication) và chuyển sang định dạng Parquet/Iceberg.
* **Tầng Gold (Analytics):** Chứa các Business Marts phục vụ Dashboard. Dữ liệu được tổ chức theo chiều thời gian và không gian.

## 2.2. Stack Công nghệ (Tech Stack)

| Thành phần | Công nghệ |
| :--- | :--- |
| **Storage** | MinIO (S3 Compatible) + Apache Iceberg |
| **Processing** | Apache Spark (Batch) + Trino (Ad-hoc Query) |
| **Orchestration** | Airflow 2.10.2 (Pinned Stable) |
| **Transformation** | dbt (Data Build Tool) |
| **Security** | HashiCorp Vault (Secrets), Keycloak (Auth) |
| **BI/Reporting** | Apache Superset |

## 2.3. Cấu trúc Lưu trữ (MinIO)

* `s3://tourism-bronze/`: Dữ liệu JSON gốc.
* `s3://tourism-silver/warehouse/`: Bảng Iceberg đã làm sạch.
* `s3://tourism-gold/warehouse/`: Các bảng Marts cuối cùng.
