# 🚀 Hướng dẫn vận hành: Hanoi Ingestion Minimal Stack

Tài liệu này hướng dẫn cách chạy cụm dịch vụ tối giản (Minimal Stack) để thu thập dữ liệu từ các nguồn (OSM, Tripadvisor, Google, Weather) mà không làm quá tải hệ thống.

---

## 1. Khởi động hệ thống Tối giản (Minimal Mode)

Thay vì chạy toàn bộ project (40+ containers), chúng ta chỉ chạy các dịch vụ cốt lõi sau:
- **Core**: Postgres, Redis, MinIO
- **Processing**: Spark (Master/Worker)
- **Orchestration**: Airflow (Webserver, Scheduler, Worker)

### Lệnh khởi động (One-liner):
```powershell
# Dừng sạch mọi Profile đang chạy
docker compose --profile "*" down

# Khởi động cụm Ingestion
docker compose up -d postgres redis minio spark-master spark-worker-1 airflow-webserver airflow-scheduler airflow-worker airflow-init
```

---

## 2. Truy cập & Vận hành

Sau khi khởi động khoảng 1-2 phút, bạn hãy truy cập các địa chỉ sau:

| Dịch vụ | Địa chỉ | Chú thích |
| :--- | :--- | :--- |
| **Airflow UI** | [http://localhost:8085](http://localhost:8085) | **Giao diện chính để chạy thu thập dữ liệu** |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | Xem dữ liệu Raw JSON đã thu thập |
| **Spark UI** | [http://localhost:8088](http://localhost:8088) | Theo dõi quá trình chạy Script |

---

## 3. Cách Trigger thu thập dữ liệu

1.  Truy cập **Airflow UI** (:8085).
2.  Tìm các DAG có tiền tố `bronze_hanoi_`:
    *   `bronze_hanoi_osm_dag`: Thu thập dữ liệu bản đồ.
    *   `bronze_hanoi_tripadvisor_dag`: Thu thập dữ liệu địa điểm du lịch.
    *   `bronze_hanoi_google_dag`: Thu thập dữ liệu đánh giá & thời tiết.
3.  Nhấn nút **Play (Trigger DAG)** để bắt đầu chạy.

---

## 4. Kiểm tra kết quả trong MinIO

Khi DAG báo **Success**, dữ liệu thô sẽ nằm ở:
1.  Vào [http://localhost:9001](http://localhost:9001).
2.  Đăng nhập bằng: `minio` / `minio123` (Cấu hình trong `.env`).
3.  Vào bucket **`tourism-bronze`**.
4.  Bạn sẽ thấy cấu trúc: `landing/<nguon>/<nam>/<thang>/<ngay>/data_xyz.json`.

---

## 5. Lưu ý về tài nguyên (RAM/CPU)

*   **Không bật thêm Profile khác** (như `monitoring`, `datahub`) khi đang làm Ingestion nếu máy yếu.
*   Nếu máy bị chậm, hãy chạy `docker system prune -f` để giải phóng cache.
*   Mọi thông số cổng đều có thể điều chỉnh tại tệp `.env`.

---
> [!IMPORTANT]
> **Tài khoản Airflow mặc định**:
> - **Username**: `admin`
> - **Password**: `admin`
