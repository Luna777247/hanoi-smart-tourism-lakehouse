# Kế hoạch Triển khai & Kiểm thử Giai đoạn 2: Ingestion Layer (Hybrid)

Tài liệu này hướng dẫn các bước để triển khai và xác minh Giai đoạn 2 của hệ thống Data Lakehouse: Tầng thu thập dữ liệu sử dụng chiến lược Hybrid (Batch & Streaming).

## 1. Thành phần Ingestion

| Loại | Công nghệ | Nguồn dữ liệu | Mục tiêu (Landing/Bronze) |
|---|---|---|---|
| **Batch** | Airflow + Spark | Google Places, TripAdvisor, OSM APIs | MinIO (JSON) & Iceberg (Bronze) |
| **Streaming** | Debezium + Kafka | Postgres (CDC) | Kafka Topics & Spark Streaming (Iceberg) |

## 2. Các bước triển khai

### Bước 1: Khởi động Hạ tầng
Sử dụng profile `travel` để đảm bảo các dịch vụ cần thiết cho Giai đoạn 2 hoạt động.
```powershell
.\manage_infra.ps1 -Action start-tourism
```

### Bước 2: Thiết lập Streaming (CDC)
Kích hoạt Debezium connector để theo dõi các thay đổi từ database `demo` trong Postgres.

1. Kiểm tra trạng thái Debezium:
   ```bash
   curl -s http://localhost:8083 | jq
   ```
2. Gửi cấu hình connector:
   ```bash
   curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
     http://localhost:8083/connectors/ -d @infra/debezium/config/demo-postgres.json
   ```

### Bước 3: Kiểm thử Batch Ingestion (Airflow)
Kích hoạt các DAG thu thập dữ liệu từ API.

1. Truy cập Airflow UI: `http://localhost:8080` (admin/admin).
2. Unpause và Trigger các DAG:
   - `bronze_hanoi_google`
   - `bronze_hanoi_tripadvisor`
   - `bronze_hanoi_osm`

*Lưu ý: Đảm bảo các API Key đã được điền trong file `.env`.*

## 3. Xác minh kết quả

- **Xác minh File (Landing Zone):** Kiểm tra MinIO (port 9001) xem có dữ liệu JSON trong các folder tương ứng không.
- **Xác minh Kafka (Streaming):** Sử dụng Kafka-UI (port 8082) để xem các tin nhắn CDC trong các topic `demo.public.*`.
- **Xác minh Bronze (Iceberg):** Sử dụng Spark History Server hoặc Trino (port 8081) để truy vấn bảng `iceberg.bronze.hanoi_attractions`.

---
> [!IMPORTANT]
> Vì hệ thống sử dụng nhiều tài nguyên (RAM/CPU), hãy đảm bảo Docker được cấp tối thiểu 16GB RAM để quá trình xử lý không bị treo.
