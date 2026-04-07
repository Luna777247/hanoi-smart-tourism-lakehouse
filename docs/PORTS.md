# 🗺️ Project Port Map - Hanoi Smart Tourism Lakehouse

Hệ thống sử dụng rất nhiều dịch vụ, dưới đây là bảng tra cứu cổng (Port) tập trung để tránh xung đột:

| Service Name | Host Port (Windows) | Container Port | URL Truy cập (Browser) | Profile tương ứng |
| :--- | :--- | :--- | :--- | :--- |
| **Lakehouse Portal** | `3100` | 80 | [http://localhost:3100](http://localhost:3100) | `portal` |
| **Airflow Main UI** | `8085` | 8080 | [http://localhost:8085](http://localhost:8085) | `airflow` |
| **Airflow Ingestion** | `8087` | 8080 | [http://localhost:8087](http://localhost:8087) | `openmetadata` |
| **Trino Coordinator** | `8080` | 8080 | [http://localhost:8080](http://localhost:8080) | `trino`, `core` |
| **MinIO Console** | `9001` | 9001 | [http://localhost:9001](http://localhost:9001) | `core` |
| **MinIO S3 API** | `9000` | 9000 | [http://localhost:9000](http://localhost:9000) | `core` |
| **Spark Master UI** | `8088` | 8080 | [http://localhost:8088](http://localhost:8088) | `core` |
| **Keycloak IAM** | `18084` | 8080 | [http://localhost:18084](http://localhost:18084) | `core` |
| **Superset BI** | `8089` | 8088 | [http://localhost:8089](http://localhost:8089) | `superset` |
| **PostgreSQL** | `15432` | 5432 | - | `core` |
| **Kafka UI** | `8082` | 8080 | [http://localhost:8082](http://localhost:8082) | `core` |
| **Debezium UI** | `8096` | 8080 | [http://localhost:8096](http://localhost:8096) | `core` |

---
> [!TIP]
> **Điểm truy cập duy nhất**: Hãy ưu tiên dùng **Lakehouse Portal (cổng 3100)**. Các dịch vụ như Airflow, Trino, MinIO đều có thể được truy cập tập trung qua các đường dẫn (paths) trên Portal này mà không cần nhớ từng cổng riêng lẻ.
