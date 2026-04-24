# BÁO CÁO DỰ ÁN: XÂY DỰNG NỀN TẢNG DỮ LIỆU THÔNG MINH CHO DU LỊCH HÀ NỘI

(HANOI SMART TOURISM DATA LAKEHOUSE PLATFORM)

---

## CHƯƠNG 1: GIỚI THIỆU DỰ ÁN

### 1.1. Tổng quan dự án

Hanoi Smart Tourism Data Lakehouse Platform là nền tảng quản trị dữ liệu quy mô lớn, được xây dựng để giải quyết bài toán tối ưu hóa tài nguyên du lịch Thủ đô. Nền tảng tập trung thu thập và phân tích dữ liệu từ hơn 2.400 điểm quan tâm (POI), cung cấp cái nhìn 360 độ về sức khỏe ngành du lịch thông qua các chỉ số thực tế từ cộng đồng du khách toàn cầu.

### 1.2. Bối cảnh và tính cấp thiết

Thực trạng quản lý du lịch hiện nay tại Hà Nội đang đối mặt với các "điểm nghẽn" dữ liệu cụ thể:

- **Sự lệch pha dữ liệu**: Các báo cáo hành chính có độ trễ lớn (1-3 tháng), không phản ánh kịp thời các biến động thực tế.
- **Dữ liệu phi cấu trúc**: Thiếu hạ tầng khai thác đánh giá từ các nền tảng xuyên biên giới (Google Maps, Tripadvisor).
- **Áp lực quá tải**: Thiếu khả năng dự báo mật độ du khách dựa trên tín hiệu quan tâm (Interest Signals).

---

## CHƯƠNG 2: KIẾN TRÚC VÀ CÔNG NGHỆ

### 2.1. Kiến trúc Medallion Lakehouse

Hệ thống được tổ chức theo mô hình Medallion trên nền tảng **Apache Iceberg**:

- **Bronze**: Ingestion thô từ API OSM và Google.
- **Silver**: Làm sạch dữ liệu, khử trùng lặp và chuẩn hóa địa giới hành chính (30 quận/huyện).
- **Gold**: Tính toán Popularity Index và xây dựng báo cáo phân tích.

### 2.2. Stack Công nghệ Ổn định (Stabilized Tech Stack)

Hệ thống đã được tinh chỉnh và đóng băng ở các phiên bản ổn định nhất:

- **Orchestration**: Apache Airflow 2.10.2 (Pinned stable) để tránh các lỗi không tương thích của bản 3.0.
- **Governance**: OpenMetadata 1.12.0 phối hợp với Elasticsearch 7.17.13.
- **Processing**: Apache Spark 3.5.0 + Trino 435.
- **Storage**: MinIO (S3-compatible).

---

## CHƯƠNG 3: QUẢN TRỊ VÀ KHÁM PHÁ DỮ LIỆU (GOVERNANCE)

### 3.1. Triển khai OpenMetadata

Hệ thống tích hợp OpenMetadata để giải quyết bài toán **Data Discovery** và **Data Lineage**:

- **Search Index**: Hệ thống sử dụng chỉ mục tổng hợp `all` trong Elasticsearch để cho phép tìm kiếm xuyên suốt các thực thể dữ liệu (Table, Pipeline, Topic).
- **Khắc phục lỗi nảy sinh**: Đã xử lý triệt để lỗi "Failed to find index all" bằng cách hạ cấp engine tìm kiếm xuống Elasticsearch 7.17.13 và vô hiệu hóa cơ chế kiểm tra sản phẩm nghiêm ngặt của ES 8.x.

### 3.2. Data Lineage

Toàn bộ quy trình từ Bronze đến Gold được giám sát tự động, cho phép truy vết nguồn gốc dữ liệu khi có sai lệch chỉ số Popularity Index.

---

## CHƯƠNG 4: TRIỂN KHAI VÀ KẾT QUẢ THỰC NGHIỆM

### 4.1. Hiệu năng Pipeline

- **In-Memory Transformation**: Xử lý 2.400 bản ghi trong < 2 phút nhờ tối ưu hóa Spark DataFrame API.
- **API Optimization**: Sử dụng kỹ thuật Batch Query và Mirror Rotation để tối ưu hóa hạn mức API từ các nhà cung cấp dữ liệu.

### 4.2. Độ tin cậy hệ thống

Hệ thống đã vượt qua các bài kiểm thử stress-test về tài nguyên (RAM/Disk I/O) thông qua việc phân luồng khởi động hệ thống theo Layer (Foundation -> Orchestration -> Governance).

---

## TÀI LIỆU THAM KHẢO

1. Sở Du lịch Hà Nội, Báo cáo tổng kết công tác du lịch năm 2024.
2. Apache Airflow 2.10.2 Official Documentation.
3. OpenMetadata 1.12.0 Deployment Guide.
4. Apache Iceberg: The Definitive Guide (O'Reilly).
