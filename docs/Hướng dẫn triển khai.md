# Hướng Dẫn Triển Khai: Hanoi Smart Tourism Lakehouse Platform

Tài liệu cung cấp các bước thiết lập và khởi chạy toàn bộ nền tảng Data Lakehouse cho dự án du lịch thông minh Hà Nội. Hệ thống được đóng gói Container hóa hoàn toàn, đảm bảo khả năng chạy liền mạch trên bất kì Desktop hay Server nào.

## 1. Yêu cầu hệ thống (Prerequisites)

- **Hệ điều hành:** Linux, macOS, hoặc Windows (có WSL2).
- **Phần mềm:**
  - Docker Engine & Docker Compose (Phiên bản mới nhất).
  - Khuyến nghị RAM tối thiểu **16GB** (Nếu chạy full stack bao gồm cả Trino, ClickHouse thì cần **32GB**).
  - Tối thiểu 50GB ổ cứng trống.
- **Port khả dụng:** Cần đảm bảo các port `8080`, `9000`, `9001` (MinIO), `5432` (Postgres), `8088` (Superset) mặc định đang không bị chiếm dụng.

## 2. Khởi tạo cấu hình ban đầu

1. Clone mã nguồn về máy:

   ```bash
   git clone <URL_REPO_CUA_BAN>
   cd hanoi-smart-tourism-lakehouse
   ```

2. Copy và điền các biến cấu hình:
   Hệ thống yêu cầu các API Key bí mật để truy cập dữ liệu thực tế:

   ```bash
   # Bắt buộc đổi tên và cấu hình
   cp .env.example .env
   ```

   *Lưu ý mở `.env` và điền: `GOOGLE_PLACES_API_KEY`, `SERPAPI_KEY`, v.v.*

## 3. Khởi chạy hệ thống

Dự án này sử dụng tính năng **Docker Compose Profiles** để cho phép bạn khởi chạy linh hoạt từng phần cứng hệ thống nhằm tiết kiệm RAM.

### Lựa chọn 1: Chạy lõi Lakehouse Du Lịch (Được khuyến nghị)

Khởi động cơ sở hạ tầng nền tảng (Postgres, MinIO), hệ thống lên lịch Airflow và cụm công nghệ lõi xử lý.

**🖥️ Nếu dùng Windows (PowerShell):**

```powershell
.\manage.ps1 up-foundation
.\manage.ps1 up-orchestration
.\manage.ps1 up-etl
```

**🐧 Nếu dùng Linux / Mac (Bash):**

```bash
docker compose up -d postgres redis minio vault spark-master spark-worker trino airflow-webserver
```

### Lựa chọn 2: Chạy TOÀN BỘ hệ thống (Full Stack)

Khởi chạy mọi thứ bao gồm Governance (OpenMetadata) và các ứng dụng Portal.
**🖥️ Windows:**

```powershell
.\manage.ps1 up
```

## 4. Quản trị và Truy cập các dịch vụ

Sau khi các Container báo trạng thái "Running", bạn cần chờ khoảng **2-3 phút** để quá trình cài đặt cơ sở dữ liệu nội bộ (Airflow Meta, Hive Metastore) hoàn tất. Sau đó, truy cập bằng trình duyệt web:

| Tên Dịch Vụ | Thành Phần Kiến Trúc | Địa chỉ (URL) | Tài khoản (Mặc định) |
|---|---|---|---|
| **MinIO Console** | Bronze / Storage Layer | <http://localhost:9001> | `minioadmin` / `minioadmin123` |
| **Airflow UI** | Orchestration Layer | <http://localhost:8080> | `admin` / `admin` |
| **Thrift/Trino UI**| Query Engine Layer | <http://localhost:8081> | - |
| **Apache Superset**| Analytics / Gold Layer | <http://localhost:8088> | `admin` / `admin` |
| **OpenMetadata** | Data Governance | <http://localhost:8585> | `admin@openmetadata.org` / `admin` |

*(Lưu ý: Bạn có thể thay đổi pass mặc định trong file `.env`)*

## 5. Dừng hệ thống & Xóa dữ liệu

Để tắt cụm an toàn giúp không bị corrupt dữ liệu Iceberg:

```powershell
# Trên Windows
.\manage.ps1 down

# Trên Linux/Mac
docker compose down
```

⚠️ *Nếu muốn xóa sạch toàn bộ databases và storage (làm lại từ đầu), sử dụng lệnh `docker-compose down -v`*

---
**Troubleshooting (Xử lý sự cố thường gặp):**

- **Q:** Airflow Webserver báo lỗi không lên web?
  **A:** Thường do thiếu RAM. Thử chạy lại `.\manage_infra.ps1 -Action build` hoặc tăng cấu nguyên bộ nhớ Docker lên.
- **Q:** DAG báo lỗi khi chạy API?
  **A:** Hãy kiểm tra lại chắc chắn bạn đã điền API Key vào `.env`.
