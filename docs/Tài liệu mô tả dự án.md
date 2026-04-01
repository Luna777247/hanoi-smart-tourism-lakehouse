# Hanoi Smart Tourism Data Lakehouse Platform

## 1. Giới thiệu dự án

**Hanoi Smart Tourism Data Lakehouse Platform** là một nền tảng dữ liệu hiện đại hỗ trợ quản lý và phát triển du lịch bền vững tại Thủ đô Hà Nội. Dự án tập trung vào thu thập, xử lý, lưu trữ và phân tích dữ liệu về các điểm du lịch (attractions) từ nhiều nguồn khác nhau, giúp quản lý theo dõi hiệu suất thực tế của từng điểm du lịch qua các chỉ số như rating, số lượng đánh giá, xu hướng thay đổi, phân bố theo quận/huyện và nguy cơ quá tải du khách.

Nền tảng được thiết kế theo kiến trúc **Data Lakehouse** hiện đại với mô hình **Medallion Architecture (Bronze → Silver → Gold)**, kết hợp tự động hóa quy trình và các công cụ quản trị dữ liệu chuyên sâu.

## 2. Mục tiêu chính
- Xây dựng quy trình thu thập dữ liệu tự động từ các API công khai (Google Local API, Tripadvisor).
- Xây dựng Data Lakehouse hoàn chỉnh, mở rộng, xử lý batch hiệu quả, truy vấn nhanh.
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

## 5. Tính năng nổi bật
### Management Portal
- Quản lý API Connections và Scheduling
- Theo dõi lịch sử thực thi pipeline
- Monitoring tình trạng hệ thống (Airflow, Spark, MinIO)
- Link nhanh đến Superset Dashboard

### Analytics Dashboard (Superset)
- Bản đồ tương tác hiển thị tất cả điểm du lịch tại Hà Nội
- KPI tổng quan (số điểm du lịch, rating trung bình, tổng review…)
- Phân tích hiệu suất theo điểm, theo quận/huyện và loại hình
- Xu hướng rating và review theo thời gian
- Cảnh báo điểm du lịch có nguy cơ overcrowding
- Giám sát Data Quality Trends

### Data Governance
- Sử dụng OpenMetadata để quản lý metadata, hiển thị Lineage rõ ràng và tích hợp kết quả Data Quality từ Great Expectations.

## 6. Tech Stack

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

## 7. Ý nghĩa thực tiễn
- Phát hiện sớm, điều tiết quá tải tại các điểm du lịch nóng (Hồ Gươm, Phố Cổ…)
- Cải thiện chất lượng dịch vụ dựa trên phản hồi thực tế của du khách
- Cung cấp cơ sở dữ liệu đáng tin cậy cho marketing, đầu tư hạ tầng, phát triển sản phẩm mới
- Thúc đẩy chuyển đổi số trong quản lý du lịch, chuyển từ truyền thống sang quản lý dựa trên dữ liệu
