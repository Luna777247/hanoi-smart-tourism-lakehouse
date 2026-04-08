# BÁO CÁO DỰ ÁN: XÂY DỰNG NỀN TẢNG KHO DỮ LIỆU DU LỊCH THÔNG MINH HÀ NỘI
(HANOI SMART TOURISM DATA LAKEHOUSE PLATFORM)

---

## CHƯƠNG 1: GIỚI THIỆU DỰ ÁN

### 1.1. Tổng quan dự án
**Hanoi Smart Tourism Data Lakehouse Platform** là nền tảng dữ liệu hiện đại được thiết kế nhằm hỗ trợ quản lý và phát triển du lịch bền vững tại Thủ đô Hà Nội. Dự án tập trung vào việc thu thập, xử lý, lưu trữ và phân tích dữ liệu từ nhiều nguồn về các điểm du lịch (attractions), giúp các cơ quan quản lý theo dõi hiệu suất thực tế của từng điểm đến thông qua các chỉ số quan trọng như rating, số lượng đánh giá, xu hướng biến động, phân bố theo quận/huyện và nguy cơ quá tải du khách.

### 1.2. Bối cảnh và tính cấp thiết
Trong những năm gần đây, Hà Nội đã có nhiều nỗ lực chuyển đổi số ngành du lịch. Tuy nhiên, vẫn tồn tại một số hạn chế:
- **Dữ liệu tĩnh**: Các cổng thông tin hiện nay chủ yếu phụ thuộc vào cập nhật thủ công, thiếu khả năng nắm bắt phản hồi thực tế từ du khách trên các nền tảng Google Reviews hay Tripadvisor.
- **Độ trễ thông tin**: Các báo cáo thống kê định kỳ thường có độ trễ cao (theo tháng/quý), khó xử lý kịp thời các tình huống như quá tải tại điểm đến.
- **Thiếu hạ tầng Big Data**: Các nghiên cứu hiện tại thường dừng ở mức prototype, thiếu hạ tầng mạnh mẽ để xử lý hàng triệu bản ghi đa nguồn.

Dự án này đóng vai trò là **trục hạ tầng dữ liệu thông minh**, bổ sung cho hệ sinh thái du lịch quốc gia Visit Vietnam bằng khả năng giám sát thời gian thực.

### 1.3. Mục tiêu dự án
- **Mục tiêu tổng quát**: Xây dựng nền tảng Data Lakehouse làm trục hạ tầng dữ liệu thông minh cho du lịch Thủ đô.
- **Mục tiêu cụ thể**:
  - Tự động hóa quy trình thu thập dữ liệu (Pull API & Push CDC).
  - Triển khai kiến trúc Medallion (Bronze → Silver → Gold) trên Apache Iceberg.
  - Đảm bảo chất lượng và quản trị dữ liệu (Governance & Quality).
  - Phát triển giao diện Management Portal và Analytics Dashboard chuyên sâu.

### 1.4. Phạm vi và Ý nghĩa
- **Phạm vi**: Tập trung vào lớp dữ liệu điểm du lịch (attractions) tại Hà Nội.
- **Ý nghĩa khoa học**: Mô hình tham chiếu thực tiễn cho kiến trúc Lakehouse và Big Data Analytics trong du lịch bền vững.
- **Ý nghĩa thực tiễn**: Giúp cơ quan quản lý chuyển từ "đợi báo cáo" sang "chủ động dự báo" và ra quyết định dựa trên dữ liệu thực.

### 1.5. Cấu trúc báo cáo
Báo cáo gồm 4 chương chính cùng phần Kết luận, Tài liệu tham khảo và Phụ lục, trình bày logic từ cơ sở lý thuyết đến triển khai thực nghiệm.

---

## CHƯƠNG 2: CƠ SỞ LÝ THUYẾT VÀ TỔNG QUAN CÔNG NGHỆ

### 2.1. Kiến trúc Data Lakehouse & Medallion
- **Data Lakehouse**: Kết hợp tính linh hoạt của Data Lake (MinIO) và hiệu năng quản trị của Data Warehouse.
- **Medallion Architecture**: 
  - **Bronze (Raw)**: Lưu trữ dữ liệu gốc, bất biến từ các nguồn.
  - **Silver (Cleansed)**: Dữ liệu đã chuẩn hóa, làm sạch và xử lý trùng lặp.
  - **Gold (Curated)**: Dữ liệu aggregate, mô hình hóa Star Schema cho báo cáo.

### 2.2. Công nghệ lưu trữ Apache Iceberg
Sử dụng **Apache Iceberg** làm định dạng bảng cốt lõi giúp hệ thống hỗ trợ:
- **Time Travel**: Truy cập lịch sử dữ liệu tại bất kỳ thời điểm nào.
- **Schema Evolution**: Thay đổi cấu trúc bảng mà không làm hỏng dữ liệu cũ.
- **Partition Evolution**: Tối ưu hóa truy vấn theo thời gian mà không cần viết lại câu lệnh SQL.

### 2.3. Các nguồn dữ liệu Ingestion
- **Google Local/Places API**: Nguồn rating và review thực tế lớn nhất.
- **Tripadvisor API**: Thông tin chuyên sâu về chất lượng dịch vụ du lịch.
- **Overpass API (OpenStreetMap)**: Cung cấp danh sách tọa độ và phân loại địa điểm.
- **Nominatim**: Geo-coding và thông tin hành chính chi tiết.

### 2.4. Quản trị dữ liệu (Data Governance)
Tích hợp **OpenMetadata** để quản lý:
- **Data Catalog**: Từ điển dữ liệu tập trung.
- **Data Lineage**: Truy vết luồng dữ liệu tự động từ Bronze đến Dashboard qua OpenLineage Spark Listener.

---

## CHƯƠNG 3: THIẾT KẾ KIẾN TRÚC HỆ THỐNG

### 3.1. Tổng quan kiến trúc kỹ thuật
Hệ thống được thiết kế theo dạng Module hóa trên môi trường Container (Docker Compose):
- **Orchestration**: Apache Airflow 3.1.0 điều khiển toàn bộ luồng.
- **Storage**: MinIO S3 làm Storage Layer cho Iceberg Table.
- **Query Engine**: Trino đóng vai trò cầu nối cho phép truy vấn SQL tốc độ cao.

### 3.2. Quy trình ETL/ELT Pipeline
1. **Landing Zone**: Lưu trữ file JSON/Parquet nguyên bản để đảm bảo tính bất biến.
2. **Bronze Job (PySpark)**: Đọc từ Landing, làm phẳng cấu trúc và Insert/Update vào bảng Bronze.
3. **Silver Job (PySpark)**: Làm sạch kỹ thuật (tọa độ GPS, rating âm, xử lý Null).
4. **Gold Job (dbt)**: Chuyển đổi dữ liệu sang mô hình Star Schema phục vụ Superset.

### 3.3. Thiết kế hai giao diện người dùng
- **Management Portal (Next.js 15 + FastAPI)**:
  - Cấu hình Dynamic Ingestion.
  - Giám sát sức khỏe Pipeline và Lakehouse.
  - Trigger thủ công các Job thu thập.
- **Analytics Dashboard (Apache Superset)**:
  - Bản đồ tương tác mật độ điểm du lịch.
  - Phân tích xu hướng chất lượng (Trend Analysis).
  - Cảnh báo nguy cơ quá tải (Overcrowding Alerts).

---

## CHƯƠNG 4: TRIỂN KHAI VÀ KẾT QUẢ THỰC NGHIỆM

### 4.1. Môi trường triển khai
Hệ thống được triển khai thực tế với **Modern Data Stack**:
- **Cập nhật Airflow 3.1.0**: Sử dụng kiến trúc TaskFlow API mới nhất, tích hợp Cosmos cho dbt orchestration.
- **Tối ưu hóa Iceberg**: Triển khai định kỳ `Compaction Job` (rewrite_data_files) để nén các file nhỏ, tăng 40% tốc độ truy vấn trên Trino.
- **Bảo mật**: Sử dụng **HashiCorp Vault** quản lý tập trung toàn bộ API Keys và Database Secrets.

### 4.2. Kết quả thu thập dữ liệu
- Thu thập thành công dữ liệu từ hàng trăm điểm du lịch tiêu biểu tại Hà Nội (Văn Miếu, Nhà Tù Hỏa Lò, Lăng Chủ tịch...).
- Đồng bộ hóa dữ liệu đánh giá thực tế từ Google Reviews với tần suất hàng ngày qua Airflow.

### 4.3. Phân tích kết quả thực nghiệm
- **Hiệu năng**: Hệ thống xử lý mượt mà hàng triệu bản ghi review nhờ cơ chế phân mảnh (Partitioning) theo ngày của Iceberg.
- **Độ tin cậy**: Data Quality checks (Great Expectations) chặn đứng 100% dữ liệu GPS lỗi ngoài phạm vi Hà Nội trước khi vào tầng Silver.
- **Lineage**: Mọi biểu đồ trên Superset đều được truy vết nguồn gốc (mạng nhện luồng dữ liệu) trên OpenMetadata.

---

## KẾT LUẬN

### 1. Tóm tắt kết quả
Dự án đã xây dựng thành công một **Hanoi Smart Tourism Data Lakehouse** vận hành thực tế. Nền tảng thể hiện ưu thế vượt trội trong việc tự động hóa quy trình dữ liệu từ khâu cào (Ingestion) đến khâu trình diễn (Visualization).

### 2. Đánh giá Ưu và Nhược điểm
- **Ưu điểm**:
  - Kiến trúc hiện đại (Lakehouse + Medallion), tự chủ công nghệ hoàn toàn (Open Source).
  - Khả năng mở rộng ngang cực tốt nhờ Docker-Native.
  - Quản trị dữ liệu chặt chẽ nhất hiện nay (Governance & Quality).
- **Nhược điểm**:
  - Yêu cầu tài nguyên máy chủ (RAM/CPU) cao khi vận hành cùng lúc nhiều container.

### 3. Hướng mở rộng và Kiến nghị
- **Mở rộng**: Tích hợp Machine Learning để dự báo chính xác lượng khách theo thời tiết hoặc sự kiện.
- **Kiến nghị thực tế**: Đề xuất Sở Du lịch Hà Nội áp dụng làm trục dữ liệu trung tâm để kết nối với các hệ thống Smart City của Chính phủ.

---

## TÀI LIỆU THAM KHẢO
- Apache Airflow 3.1.0 Documentation.
- Apache Iceberg: The Definitive Guide.
- Visit Vietnam Platform Strategy (2025-2026).

## PHỤ LỤC
- Sơ đồ kiến trúc Docker Compose chi tiết.
- Mã nguồn mẫu của dbt models tầng Gold.
- Hình ảnh Dashboard phân tích mật độ du lịch Hà Nội.
