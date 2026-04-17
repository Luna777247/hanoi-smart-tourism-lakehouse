# 📂 MinIO Data Lake Structure — Hanoi Smart Tourism

Tài liệu này giải thích cấu trúc thư mục và cách tổ chức dữ liệu trong các Bucket của MinIO theo kiến trúc Medallion (Bronze, Silver, Gold).

---

## 1. Bucket: `tourism-bronze` (Raw Zone)

Nơi lưu trữ dữ liệu thô (immutable) và dữ liệu lịch sử chưa qua xử lý.

### 📁 `source={source_name}/`

Chứa các file JSON thô được kéo từ các API hoặc Web Scraping qua Airflow.

* **Cấu trúc chính:** `source=osm_google_enriched/snapshot_date={YYYY-MM-DD}/run_id={RUN_ID}/data.json`
* **Luồng ingestion chính duy nhất (`source_name`):**
  * `osm_google_enriched`: Dữ liệu OpenStreetMap đã được làm giàu thông tin từ Google, được Airflow ingest lúc `02:00` mỗi ngày.

---

## 2. Bucket: `tourism-silver` (Cleaned Zone)

Nơi lưu trữ dữ liệu đã được làm sạch, chuẩn hóa, khử trùng lặp và gán schema chuẩn.

### 📁 `attractions/` (Iceberg Table)

Bảng Iceberg chứa thông tin địa điểm du lịch đã được chuẩn hóa.

* **Cấu trúc:** Dữ liệu được phân vùng (partition) theo ngày xử lý (`partition_date` hoặc `cleaned_at`).
* **Vị trí:** `s3a://tourism-silver/attractions/`

### 📁 `attractions_enriched/` (Iceberg Table)

Bảng Silver chính của hệ thống, kết hợp dữ liệu OSM và Google sau bước làm giàu và khử trùng lặp.

* **Vị trí:** `s3a://tourism-silver/attractions_enriched/`

### 📁 `warehouse/`

Thư mục mặc định của Spark Iceberg Catalog cho các bảng không chỉ định location cụ thể.

---

## 3. Bucket: `tourism-gold` (Curated/Analytics Zone)

Nơi lưu trữ dữ liệu tinh chế, được tổ chức theo dạng Star Schema (Dim/Fact) để phục vụ Dashboard (Superset) và Backend.

### 📁 `mart_district_stats/` (Iceberg Table)

Mart tổng hợp theo quận/huyện từ Silver enriched.

* **Vị trí:** `s3a://tourism-gold/mart_district_stats/`

### 📁 `mart_tourism_heatmap/` (Iceberg Table)

Mart phục vụ heatmap và scoring mức độ phổ biến địa điểm.

* **Vị trí:** `s3a://tourism-gold/mart_tourism_heatmap/`

---

## 4. Các thư mục hệ thống (System Directories)

Mỗi bucket sẽ có thêm các thư mục kỹ thuật do Spark/Iceberg tạo ra:

* **`metadata/`**: Chứa các file `.metadata.json`, `manifest-list`, `manifest` của Apache Iceberg (dùng để quản lý versioning/snapshot và time travel).
* **`temp/`** hoặc **`.spark-staging/`**: Chứa dữ liệu tạm thời trong quá trình Spark Job đang chạy (sẽ tự xóa sau khi xong).

---

## Kiểu dữ liệu (Data Formats)

| Layer | Format | Công nghệ | Mục đích |
| :--- | :--- | :--- | :--- |
| **Bronze** | JSON / Parquet | Landing / Iceberg | Giữ nguyên gốc, truy vết lỗi. |
| **Silver** | Parquet | Iceberg | Truy vấn hiệu năng cao, schema chuẩn. |
| **Gold** | Parquet | Iceberg | Business Logic, BI Dashboard. |

---
*Cập nhật: 2026-04-17*
