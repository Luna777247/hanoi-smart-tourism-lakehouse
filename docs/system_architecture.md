# Sơ đồ Kiến trúc Hệ thống

Dưới đây là sơ đồ kiến trúc tổng quan của nền tảng **Hanoi Smart Tourism Data Lakehouse Platform** dựa trên tài liệu mô tả dự án. Hệ thống được thiết kế theo mô hình **Medallion Architecture 3 lớp (Bronze - Silver - Gold)** kết hợp cả xử lý Batch và Streaming (Hybrid Ingestion).

```mermaid
flowchart TD
    %% Định dạng phong cách
    classDef source fill:#f9d0c4,stroke:#333,stroke-width:2px;
    classDef ingestion fill:#f6e58d,stroke:#333,stroke-width:2px;
    classDef storage fill:#c7ecee,stroke:#333,stroke-width:2px;
    classDef process fill:#dff9fb,stroke:#333,stroke-width:2px;
    classDef serve fill:#badc58,stroke:#333,stroke-width:2px;
    classDef govern fill:#fff200,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5;

    %% Data Sources
    subgraph Nguồn_Dữ_Liệu [Data Sources]
        direction LR
        API1[Google Places API]
        API2[Tripadvisor API]
        API3[OSM APIs\nOverpass + Nominatim]
        DB1[(Cơ Sở Dữ Liệu Nội Bộ\nOracle / Postgres)]
    end
    class API1,API2,API3,DB1 source;

    %% Ingestion Layer
    subgraph Thu_Thập [Ingestion Layer]
        Airflow[Apache Airflow\nOrchestrator & API Pull]
        Debezium[Debezium CDC]
        Kafka[Apache Kafka\nMessage Broker]
    end
    class Airflow,Debezium,Kafka ingestion;

    API1 -->|REST API - Lên lịch| Airflow
    API2 -->|REST API - Lên lịch| Airflow
    API3 -->|REST API - Lên lịch| Airflow
    
    DB1 -->|Bắt thay đổi WAL/Binlog| Debezium
    Debezium -->|Tin nhắn thời gian thực| Kafka

    %% Landing Zone
    Landing[(MinIO S3 Landing Zone\nJSON Dữ liệu Gốc)]
    class Landing storage;

    Airflow -->|Lưu trực tiếp JSON| Landing

    %% Lakehouse Processing
    subgraph Medallion_Lakehouse [Medallion Lakehouse Architecture]
        direction TB
        
        SparkBatch[PySpark Batch Processing\nLàm phẳng & Ghi Iceberg]
        SparkStream[Spark / Flink Streaming]
        
        Bronze[(Bronze Layer\nRaw Data - Iceberg)]
        Silver[(Silver Layer\nCleaned - Iceberg)]
        Gold[(Gold Layer\nAggregated - Iceberg)]
        
        DBT[dbt\nData Build Tool]
        
        Landing --> SparkBatch
        Kafka --> SparkStream
        
        SparkBatch --> Bronze
        SparkStream --> Bronze
        
        Bronze -->|PySpark: Làm sạch, Chuẩn hóa| Silver
        Silver -->|dbt: Tổng hợp, Star Schema| Gold
    end
    class Bronze,Silver,Gold storage;
    class SparkBatch,SparkStream,DBT process;

    %% Serving
    subgraph Tiêu_Thụ [Serving & Consumption]
        Trino[Trino\nTruy vấn phân tán tốc độ cao]
        Gold --> Trino
        
        Superset[Apache Superset\nAnalytics Dashboard]
        Portal[FastAPI + Next.js\nManagement Portal]
        
        Trino -->|Truy vấn SQL| Superset
        Trino -->|Truy vấn SQL| Portal
    end
    class Trino,Superset,Portal serve;

    %% Data Quality & Governance
    subgraph Quản_Trị [Data Governance & Quality]
        GX[Great Expectations\nData Quality Checks]
        OpenMeta[OpenMetadata\nData Catalog & Lineage]
    end
    class GX,OpenMeta govern;

    %% Connections for Governance
    Silver -. "Kiểm định chất lượng" .-> GX
    Gold -. "Kiểm định chất lượng" .-> GX
    Bronze -. "Thu thập Lineage" .-> OpenMeta
    Silver -. "Thu thập Lineage" .-> OpenMeta
    Gold -. "Thu thập Lineage" .-> OpenMeta
    Airflow -. "Gửi trạng thái Pipeline" .-> OpenMeta
```

### Thành phần chính

1. **Nguồn dữ liệu (Data Sources):** Dữ liệu thu thập từ API công khai thiết lập bằng cơ chế **Pull** và dữ liệu nội bộ qua cơ chế **Push (CDC)**. 
2. **Thu thập (Ingestion Layer):** Sử dụng chiến lược Hybrid: Batch (Airflow kéo API) và Streaming (Debezium + Kafka).
3. **Lưu trữ & Xử lý (Medallion Lakehouse):** Minh họa rõ quá trình dữ liệu đi từ MinIO Landing Zone, biến đổi phẳng hoá qua PySpark vào bảng Iceberg Bronze, làm sạch ở Silver và tổng hợp bằng dbt tại layer Gold.
4. **Tiêu thụ (Consumption):** Lớp Gold được truy vấn bởi Trino với tốc độ cực nhanh, từ đó trực quan trên Apache Superset và hiển thị trên Portal do Next.js phát triển.
5. **Quản trị & Chất lượng (Governance):** Song song với các layer xử lý, OpenMetadata tự động ghi nhận quá trình di chuyển của luồng dữ liệu (Lineage), còn Great Expectations thực thi bài kiểm tra về độ chuẩn xác của dữ liệu.
