# Hanoi Smart Tourism Data Lakehouse Platform

## 1. Giới thiệu dự án

**Hanoi Smart Tourism Data Lakehouse Platform** là một nền tảng dữ liệu hiện đại hỗ trợ quản lý và phát triển du lịch bền vững tại Thủ đô Hà Nội. Dự án tập trung vào thu thập, xử lý, lưu trữ và phân tích dữ liệu về các điểm du lịch (attractions) từ nhiều nguồn khác nhau, giúp quản lý theo dõi hiệu suất thực tế của từng điểm du lịch qua các chỉ số như rating, số lượng đánh giá, xu hướng thay đổi, phân bố theo quận/huyện và nguy cơ quá tải du khách.

Nền tảng được thiết kế theo kiến trúc **Data Lakehouse** hiện đại với mô hình **Medallion Architecture (Bronze → Silver → Gold)**, kết hợp tự động hóa quy trình và các công cụ quản trị dữ liệu chuyên sâu.

## 2. Mục tiêu chính
- Xây dựng quy trình thu thập(insert + update theo id) dữ liệu tự động từ các API công khai (Google Local API, Tripadvisor, overpassapi(lấy danh sách) + Nomation(lấy thông tin chi tiết)) và lưu trữ vào Data Lakehouse tầng Bronze(Minio).
- Xây dựng Data Lakehouse hoàn chỉnh, mở rộng, xử lý batch hiệu quả, truy vấn nhanh, tầng silver xử lý làm sạch , loại bỏ những dữ liệu thừa và không liên quan, tầng gold xử lý tổng hợp dữ liệu để phục vụ cho việc phân tích và báo cáo.

- Đảm bảo chất lượng và quản trị dữ liệu tốt qua Data Catalog, Data Lineage, Data Quality checks.
- Cung cấp hai giao diện chuyên biệt:
  - **Management Portal**: Cho quản trị viên, data engineer.
  - **Analytics Dashboard**: Cho người dùng business (Sở Du lịch, ban quản lý điểm du lịch).
- Hỗ trợ ra quyết định dựa trên dữ liệu cho ngành du lịch Hà Nội.

## 3. Phạm vi dự án (Scope)
- Thu thập thông tin chi tiết về các điểm du lịch tại Hà Nội (tên, vị trí GPS, rating, số review, loại hình, địa chỉ…).
- Xử lý, làm sạch dữ liệu từ nguồn thô đến dạng sẵn sàng phân tích.
- Phân tích hiệu suất điểm du lịch: rating trung bình, xu hướng rating, số lượng review, phân bố theo quận/huyện và loại hình.
- Phát hiện, cảnh báo các điểm du lịch có nguy cơ quá tải.
- Giám sát chất lượng dữ liệu và truy vết nguồn gốc dữ liệu (Lineage).

## 4. Kiến trúc hệ thống

### 4.1. Management Portal (Cổng quản trị trung tâm)
- **Công nghệ:** Next.js 15 + FastAPI (Python)
- **Chức năng:** Quản lý kết nối API, lập lịch pipeline, theo dõi thực thi job, giám sát hệ thống, trigger pipeline thủ công.

### 4.2. Analytics Dashboard
- **Công nghệ:** Apache Superset
- **Chức năng:** Phân tích trực quan, bản đồ tương tác, KPI, trend analysis, cảnh báo và báo cáo.

### 4.3. Kiến trúc dữ liệu: Medallion Architecture
- **Bronze Layer:** Dữ liệu thô từ API (Parquet trên MinIO + Iceberg)
- **Silver Layer:** Dữ liệu đã làm sạch, chuẩn hóa (PySpark)
- **Gold Layer:** Dữ liệu aggregate, mô hình hóa (dbt)

### 4.4. Thành phần cốt lõi
- **Orchestration:** Apache Airflow
- **Storage:** MinIO + Apache Iceberg
- **Processing:** PySpark + dbt
- **Data Governance:** OpenMetadata (Data Catalog, Lineage Bronze → Silver → Gold)
- **Data Quality:** Great Expectations
- **Query Engine:** Trino
- **Container:** Docker Compose

### 4.5. Phương pháp triển khai Ingestion (Chi tiết)

Hệ thống áp dụng chiến lược **Hybrid Ingestion** (Lai ghép) nhằm tối ưu hóa việc thu thập dữ liệu từ cả nguồn bên ngoài (Public API) và nguồn nội bộ (Database):

#### 4.5.1. API Ingestion (Cơ chế Pull - Theo lịch trình)
- **Công cụ chính:** Apache Airflow sử dụng PythonOperator.
- **Quy trình:**
  - Airflow DAGs thực hiện gọi các API (Google Places, SerpApi, TripAdvisor) theo các tham số cấu hình linh động (Bounding Box, Search Query).
  - **Landing Zone:** Dữ liệu JSON nguyên bản được lưu trực tiếp vào MinIO (Standard S3) để bảo toàn dữ liệu gốc (Data Immutability).
  - **Cơ chế Retry & Rate Limit:** Tích hợp logic xử lý lỗi mạng và giới hạn tần suất gọi API (Rate Limiting) để tránh bị khóa API Key.
  - **Metadata Driven:** Danh sách các điểm du lịch cần crawl được lưu trong database cấu hình, cho phép mở rộng quy mô thu thập mà không cần sửa code.

#### 4.5.2. CDC Ingestion (Cơ chế Push - Thời gian thực)
- **Công cụ chính:** Debezium + Apache Kafka.
- **Quy trình:**
  - **Debezium Connector:** Theo dõi log thay đổi (Binary Log/WAL) từ các cơ sở dữ liệu nguồn (Oracle, Postgres).
  - **Kafka Messaging:** Mọi thay đổi (Insert/Update/Delete) được đẩy vào các Kafka Topics dưới dạng chuỗi sự kiện.
  - **Flink/Spark Streaming:** Xử lý dòng dữ liệu thô từ Kafka, thực hiện các phép biến đổi đơn giản và ghi vào bảng Iceberg tại tầng Bronze/Silver.
  - **Ưu điểm:** Giảm tải cho database nguồn, đảm bảo dữ liệu trong Lakehouse luôn đồng bộ theo thời gian thực (Low Latency).

#### 4.5.3. Quản lý Secret & Bảo mật
- Toàn bộ API Keys (Google, SerpApi) và thông tin đăng nhập database được lưu trữ tập trung tại **HashiCorp Vault**.
- Các Ingestion Job sẽ truy xuất secret động tại thời điểm chạy thông qua `vault-entrypoint.sh`, đảm bảo không lộ thông tin nhạy cảm trong code hoặc log.

## 5. Tính năng nổi bật

### 5.1. Management Portal (Cổng quản trị tự động hóa)
*   **Quản lý kết nối API & Dynamic Ingestion:** Cho phép cấu hình các nguồn dữ liệu từ Google Local Service API và Tripadvisor API một cách linh động qua giao diện quản trị, không cần can thiệp vào code. Hỗ trợ quản lý API keys và cấu hình tham số thu thập tự động.
*   **Điều phối Pipeline (Orchestration Management):** Tích hợp sâu với Apache Airflow để khởi tạo, lập lịch và giám sát các luồng dữ liệu (DAGs) từ thu thập thô (Bronze) đến xử lý chuyên sâu (Silver/Gold). Người quản trị có thể trigger các job thủ công ngay từ portal.
*   **Giám sát trạng thái Lakehouse:** Theo dõi theo thời gian thực hiệu năng của các thành phần lưu trữ như MinIO (S3 bucket status) và Spark cluster (tỷ lệ thành công/thất bại của các job xử lý).
*   **Cổng truy cập hợp nhất (Centralized Hub):** Là điểm truy cập duy nhất tích hợp Single Sign-On (SSO) để điều hướng nhanh đến Airflow UI, Superset Dashboard và OpenMetadata Catalog, giúp tối ưu quy trình làm việc cho Data Engineer.

### 5.2. Luồng xử lý dữ liệu Medallion Architecture
*   **Bronze (Raw Layer):** Tự động hóa việc ingest dữ liệu thô định dạng Parquet với đầy đủ metadata từ API, lưu trữ an toàn trên MinIO. Sử dụng Apache Iceberg để quản lý phiên bản dữ liệu, hỗ trợ tính năng Time Travel (truy cập dữ liệu tại một thời điểm trong quá khứ).
*   **Silver (Cleansing & Standardization):** Áp dụng PySpark để làm sạch dữ liệu ở quy mô lớn, xử lý trùng lặp (deduplication), chuẩn hóa tọa độ GPS, xử lý giá trị thiếu và định dạng dữ liệu nhất quán, sẵn sàng cho các phân tích nghiệp vụ.
*   **Gold (Analytics Layer):** Sử dụng dbt để xây dựng mô hình dữ liệu (Star Schema), tính toán các bảng aggregate (tổng hợp) tối ưu cho báo cáo, giúp tăng tốc độ truy vấn trên Trino cho các dashboard phức tạp.

### 5.3. Analytics & Smart Monitoring (Superset Dashboard)
*   **Bản đồ du lịch tương tác (Interactive Map):** Trực quan hóa mật độ các điểm du lịch và đánh giá của khách tham quan trên bản đồ địa lý Hà Nội, cho phép lọc nhanh theo quận/huyện, loại hình (di tích, bảo tàng, khu vui chơi, ẩm thực...).
*   **Hệ thống cảnh báo quá tải (Overcrowding Alerts):** Thuật toán tự động phát hiện các điểm du lịch có sự gia tăng đột biến về lượng đánh giá hoặc tương tác trong thời gian ngắn, giúp cơ quan quản lý dự báo và điều tiết luồng khách.
*   **Phân tích xu hướng chất lượng (Trend Analysis):** Theo dõi sự biến động của chỉ số rating và số lượng review theo thời gian để đánh giá sức hút và chất lượng dịch vụ của từng điểm đến hoặc khu vực du lịch.

### 5.4. Data Governance & Quality (Quản trị tin cậy)
*   **Data Lineage (Truy vết nguồn gốc):** Tận dụng OpenMetadata để hiển thị sơ đồ đường đi của dữ liệu từ nguồn API thô (Bronze) xuyên suốt qua các lớp xử lý đến các bảng báo cáo cuối cùng, giúp dễ dàng cô lập và xử lý lỗi.
*   **Data Quality Automation:** Tích hợp Great Expectations vào pipeline xử lý để tự động kiểm định chất lượng dữ liệu (data validation) theo các bộ quy tắc định sẵn (ví dụ: rating 1-5, GPS hợp lệ), đảm bảo dữ liệu trên dashboard luôn tin cậy.
*   **Centralized Metadata Catalog:** Cung cấp kho từ điển dữ liệu (Data Dictionary) giúp người dùng business hiểu rõ ý nghĩa của từng chỉ số và trường thông tin trong hệ thống.

## 6. Luồng dữ liệu tổng thể (Main Workflow)
1. **Ingest (Landing & Bronze)**:
   - **Persistent Landing Zone**: Sử dụng Python (Airflow PythonOperator) để gọi API và lưu dữ liệu JSON nguyên bản vào MinIO. Điều này đảm bảo tính bất biến (immutability) và khả năng tái xử lý dữ liệu.
   - **Bronze Layer**: Sử dụng Spark job để đọc dữ liệu từ Landing Zone, làm phẳng cấu trúc và lưu vào bảng Apache Iceberg. Hỗ trợ cơ chế **Upsert (Insert + Update)** dựa trên unique ID của từng nguồn.
2. **Process (Spark):** Làm sạch, chuẩn hóa dữ liệu từ Bronze -> Ghi vào Silver Layer.
3. **Model (dbt):** Tổng hợp dữ liệu từ Silver thành các bảng báo cáo chuyên biệt -> Ghi vào Gold Layer.
4. **Govern (OpenMetadata):** Tự động cập nhật Metadata và Lineage sau mỗi lần pipeline chạy thành công.
5. **Quality (Great Expectations):** Kiểm định chất lượng tại các bước chuyển tiếp dữ liệu.
6. **Serve (Trino/Superset):** Cung cấp API truy vấn cho Portal và dữ liệu trực quan cho Dashboard.

## 7. Tech Stack

| Layer                | Công nghệ                        |
|----------------------|----------------------------------|
| Backend              | FastAPI (Python)                 |
| Frontend Portal      | Next.js 15 + TypeScript          |
| Orchestration        | Apache Airflow                   |
| Lakehouse Storage    | MinIO + Apache Iceberg           |
| Processing           | PySpark, dbt                     |
| Data Governance      | OpenMetadata                     |
| Data Quality         | Great Expectations               |
| Query Engine         | Trino                            |
| Visualization        | Apache Superset                  |
| Container            | Docker Compose                   |

## 8. Ý nghĩa thực tiễn
- **Tối ưu hóa quản lý điểm đến:** Phát hiện sớm các điểm du lịch có nguy cơ "quá tải" (overcrowding), giúp Sở Du lịch và ban quản lý có phương án điều tiết luồng khách kịp thời, bảo vệ di sản và nâng cao trải nghiệm du khách.
- **Ra quyết định dựa trên dữ liệu:** Thay thế các báo cáo thủ công bằng dữ liệu theo thời gian thực về rating, xu hướng phản hồi và mật độ khách, giúp hoạch định chiến lược đầu tư hạ tầng và marketing du lịch chính xác hơn.
- **Nâng cao chất lượng dịch vụ:** Thông qua việc theo dõi sát sao "hơi thở" của du khách trên các nền tảng số, các đơn vị kinh doanh có thể nhận diện điểm yếu trong dịch vụ để cải tiến kịp thời.
- **Thúc đẩy Chuyển đổi số:** Xây dựng một "Trục dữ liệu số" (Digital Data Backbone) cho ngành du lịch Thủ đô, sẵn sàng tích hợp với các hệ thống Smart City khác trong tương lai.
127: 
128: ## 9. So sánh với các nghiên cứu & hệ thống hiện có
129: Chi tiết về những điểm mới (Novelty) và ưu thế vượt trội của dự án so với các hệ thống du lịch thông minh và nghiên cứu hiện nay có thể xem tại: [So sánh nghiên cứu liên quan](file:///d:/hanoi-smart-tourism-lakehouse/docs/So_sanh_nghien_cuu_lien_quan.md)
