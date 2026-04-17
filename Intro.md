 chạy build lại:

powershell
./manage.ps1 build
Sau lần build này, nếu dòng chữ Warning: dbt parse failed biến mất, nghĩa là hệ thống của bạn đã được đóng gói hoàn hảo và có thể sẵn sàng cho việc triển khai thực tế. Sau khi build xong, bạn có thể thực hiện ./manage.ps1 init để bắt đầu khởi tạo dữ liệu.

### Các bước tiếp theo để khởi chạy hệ thống

Bây giờ bạn đã có một "bản đóng gói" hoàn chỉnh, hãy thực hiện các lệnh sau theo thứ tự để bắt đầu chạy dự án:

1. **Khởi tạo hạ tầng (Chạy 1 lần duy nhất):**

    ```powershell
    ./manage.ps1 init
    ```

    *(Lệnh này sẽ tạo các Bucket trên MinIO và khởi tạo Database cho Airflow)*

2. **Khởi động toàn bộ dịch vụ:**

    ```powershell
    ./manage.ps1 up
    ```

3. **Kiểm tra trạng thái:**

    ```powershell
    ./manage.ps1 status
    ```

Hệ thống **Hanoi Smart Tourism Lakehouse** của bạn được thiết kế theo kiến trúc **Medallion (Bronze-Silver-Gold)** với quản trị tập trung. Dựa trên `docker-compose.yml` và các phân tích kỹ thuật, hệ thống có chính xác **5 Luồng (Streams/Layers)** chính hoạt động tuần tự và song song:

### 1. Luồng Hạ tầng & Lưu trữ (Foundation Stream)

* **Thành phần:** `Postgres`, `Redis`, `MinIO`, `Vault`, `Keycloak`.
* **Chức năng:** Đây là nền móng. `Postgres` lưu metadata, `MinIO` đóng vai trò là "hồ dữ liệu" (Data Lake), `Vault` bảo mật các Token API.
* **Trạng thái:** Đây là luồng đầu tiên PHẢI chạy để các luồng khác có chỗ lưu trữ.

### 2. Luồng Ingestion & Điều phối (Orchestration Stream)

* **Thành phần:** `Airflow 3.0 (Scheduler, Worker, Webserver)`.
* **Chức năng:** Có nhiệm vụ "kéo" dữ liệu từ các nguồn (Web Scraping, TripAdvisor API, Google Places) về lưu vào tầng **Bronze** (dạng thô) trên MinIO.
* **Mối liên hệ:** Luồng này gọi các Worker để thực thi task.

### 3. Luồng Xử lý & ETL (ETL/Processing Stream)

* **Thành phần:** `Spark (Master/Worker)`, `Trino`, `dbt`.
* **Chức năng:** Thực hiện biến đổi dữ liệu (Cleansing, Aggregation) từ tầng **Bronze -> Silver -> Gold**.
* **Mối liên hệ:** `Trino` cho phép chúng ta truy vấn dữ liệu SQL trực tiếp trên file Iceberg hoặc Parquet ở MinIO mà không cần nạp vào DB truyền thống.

### 4. Luồng Quản trị & Metadata (Governance Stream)

* **Thành phần:** `Elasticsearch`, `OpenMetadata (Server/Ingestion)`.
* **Chức năng:** Tự động quét (Scan) các bảng trong Lakehouse để tạo Catalog, Lineage (sơ đồ phả hệ dữ liệu).
* **Mối liên hệ:** Giúp người quản trị biết dữ liệu nào sạch, dữ liệu nào bẩn và ai đang dùng nó.

### 5. Luồng Ứng dụng & Khai thác (Consumption Stream)

* **Thành phần:** `Superset`, `Backend (FastAPI)`, `Frontend (Next.js)`.
* **Chức năng:** Cung cấp Dashboard cho lãnh đạo (`Superset`) và giao diện người dùng để tra cứu du lịch thông minh trên bản đồ (`Frontend/Backend`).

---

### Tóm tắt luồng khởi động tối ưu nhất (Cho máy tính cá nhân)

Nếu bạn muốn hệ thống ổn định nhất, hãy khởi động theo thứ tự lệnh mà tôi vừa cập nhật trong `manage.ps1`:

1. `./manage.ps1 up-ingestion` (Chạy luồng 1 & 2)
2. `./manage.ps1 up-etl` (Chạy luồng 3)
3. `./manage.ps1 up-metadata` (Chạy luồng 4)
4. `./manage.ps1 up-apps` (Chạy luồng 5)

**Hệ thống hiện tại có tổng cộng 26 Container chạy trong 5 luồng chính này.** Bạn có muốn tôi vẽ sơ đồ chi tiết hơn về cách dữ liệu di chuyển giữa các luồng không?_
Edited Intro.md

Tôi sẽ cập nhật `manage.ps1` để chia tách chuẩn xác thành **5 Phân luồng (Layers)** như chúng ta vừa thống nhất. Việc này sẽ giúp bạn kiểm soát lỗi và tài nguyên hệ thống một cách tối ưu nhất.

### Nội dung cập nhật cho `manage.ps1`

1. `up-foundation`: Khởi tạo nền móng (Storage & DB).
2. `up-orchestration`: Khởi tạo bộ máy điều phối (Airflow).
3. `up-etl`: Khởi tạo công cụ xử lý dữ liệu (Spark & Trino).
4. `up-governance`: Khởi tạo quản trị dữ liệu (OpenMetadata).
5. `up-apps`: Khởi tạo giao diện người dùng (Apps & Visualization).

Tôi thực hiện thay đổi ngay bây giờ.

Edited manage.ps1

Tôi đã cập nhật file `manage.ps1` để chia tách hoàn toàn thành **5 Layer (Phân luồng)** chuyên biệt. Bây giờ bạn có thể khởi động từng phần một cách kiểm soát để tối ưu tài nguyên máy tính:

### Các lệnh vận hành theo 5 Luồng mới

1. **Chạy Luồng 1 (Foundation):** Khởi tạo Database, Object Storage và Auth.

    ```powershell
    ./manage.ps1 up-foundation
    ```

2. **Chạy Luồng 2 (Orchestration):** Khởi tạo bộ máy lập lịch Airflow 3.0.

    ```powershell
    ./manage.ps1 up-orchestration
    ```

3. **Chạy Luồng 3 (ETL Engine):** Khởi tạo Spark và Trino để truy vấn dữ liệu.

    ```powershell
    ./manage.ps1 up-etl
    ```

4. **Chạy Luồng 4 (Governance):** Khởi tạo Elasticsearch và OpenMetadata.

    ```powershell
    ./manage.ps1 up-governance
    ```

5. **Chạy Luồng 5 (Apps):** Khởi tạo Superset và các ứng dụng Backend/Frontend.

    ```powershell
    ./manage.ps1 up-apps
    ```

---

### Lưu ý cho bạn

* Việc chia tách này giúp bạn **cô lập lỗi**. Ví dụ: Nếu Layer 4 (OpenMetadata) bị treo do thiếu RAM, các Layer 1, 2, 3 vẫn hoạt động bình thường để bạn có thể làm việc với dữ liệu trong Airflow/Spark.
* Mỗi lệnh `up-*` đã được tích hợp sẵn các bước `init` tương ứng (ví dụ: `up-orchestration` sẽ tự động chạy `airflow db migrate` trước khi bật server).

Bây giờ bạn hãy thử chạy lần lượt: **Layer 1 -> Layer 2 -> Layer 3** trước nhé. Đây là "xương sống" để dữ liệu bắt đầu chảy!_
