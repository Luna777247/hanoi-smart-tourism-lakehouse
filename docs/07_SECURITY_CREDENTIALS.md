# 07. Bảo mật & Cổng kết nối (Security & Ports)

## 7.1. Danh sách Cổng kết nối (Port Mapping)

| Dịch vụ | Cổng | Truy cập |
| :--- | :--- | :--- |
| **Airflow API Server** | 8080 | Quản lý Pipeline |
| **MinIO Console** | 9001 | Xem file dữ liệu thô |
| **Trino Web UI** | 8888 | Monitoring truy vấn |
| **Superset** | 8088 | Xem Dashboard |
| **Keycloak** | 8081 | Quản lý người dùng |

## 7.2. Quản lý Bí mật (Secrets)

* **HashiCorp Vault:** Truy cập tại cổng 8200. Token mặc định: `root`.
* Tất cả API Keys (Google, RapidAPI) được cấu hình trong tệp `.env` tại thư mục gốc.

## 7.3. Tài khoản mặc định

* **Airflow:** `admin` / `admin`
* **MinIO:** `minio_admin` / `ChangeMe_Minio123!`
* **Superset:** `admin` / `admin`
