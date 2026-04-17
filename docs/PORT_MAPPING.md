# BẢN ĐỒ CỔNG VÀ KẾT NỐI (PORT MAPPING) 🗺️

Tài liệu này tổng hợp toàn bộ các cổng (Ports) công cộng và kết nối nội bộ của hệ thống **Hanoi Smart Tourism Lakehouse**.

---

## 1. GIAO DIỆN QUẢN LÝ (USER INTERFACES)

Sử dụng trình duyệt truy cập các địa chỉ sau tại `localhost`:

| Dịch vụ | Địa chỉ (Host) | Chức năng chính |
| :--- | :--- | :--- |
| **Airflow Web** | [localhost:8080](http://localhost:8080) | Điều phối Pipeline, theo dõi DAGs. |
| **Spark Master UI** | [localhost:8090](http://localhost:8090) | Giám sát các Job tính toán Spark. |
| **MinIO Console** | [localhost:9001](http://localhost:9001) | Quản lý File/Object Storage (Data Lake). |
| **Superset** | [localhost:8088](http://localhost:8088) | Dashboard phân tích du lịch. |
| **Keycloak** | [localhost:8081](http://localhost:8081) | Quản lý danh tính và quyền truy cập (IAM). |
| **OpenMetadata** | [localhost:8585](http://localhost:8585) | Quản trị dữ liệu và Lineage (Mạng nhện). |
| **Vault UI** | [localhost:8200](http://localhost:8200) | Quản lý Secrets, API Keys. |
| **Flower** | [localhost:5555](http://localhost:5555) | Giám sát các task Celery (nếu dùng). |

---

## 2. CỔNG DỊCH VỤ & API (ENGINEERING PORTS)

Các cổng dùng để kết nối bằng công cụ lập trình hoặc BI Tools:

| Dịch vụ | Cổng Host | Protocol | Ghi chú |
| :--- | :--- | :--- | :--- |
| **Trino (SQL)** | `8888` | HTTP/SQL | Kết nối từ DBeaver, Superset. |
| **PostgreSQL** | `5432` | TCP/SQL | Lưu trữ Metadata cho Airflow/Superset. |
| **MinIO API** | `9000` | S3 API | Endpoint cho Spark/Python đọc/ghi dữ liệu. |
| **Spark Master** | `7077` | TCP | Cổng giao tiếp nội bộ cho Spark Worker. |
| **Vault API** | `8200` | HTTP/API | Lấy API Keys từ code Python. |

---

## 3. KẾT NỐI NỘI BỘ (INTERNAL DNS)

Khi cấu hình mã nguồn chạy bên trong Docker (DAGs, Spark Jobs), hãy sử dụng tên Service thay vì localhost:

*   **Database**: `postgres:5432`
*   **Object Storage**: `minio:9000`
*   **Cache**: `redis:6379`
*   **Trino**: `trino:8080`
*   **Vault**: `vault:8200`
*   **OpenMetadata**: `openmetadata-server:8585`

---

## 4. TÙY CHỈNH CỔNG
Nếu bạn muốn thay đổi bất kỳ cổng nào, hãy chỉnh sửa tại file [**.env**](file:///d:/hn-smart-tourism-lakehouse/.env) ở thư mục gốc:
```env
# Ví dụ đổi cổng Airflow
AIRFLOW_WEBSERVER_PORT=8080 -> 8085
```

> [!NOTE]
> Sau khi sửa file `.env`, bạn cần chạy `.\manage.ps1 down` và `.\manage.ps1 up` để áp dụng thay đổi.
