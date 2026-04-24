# 05. Đặc tả API (API Specification)

Hệ thống cung cấp các API để truy cập dữ liệu đã làm giàu cho ứng dụng Mobile hoặc Web bên ngoài.

## 5.1. Attractions API

* **Endpoint:** `/api/v1/attractions`
* **Method:** `GET`
* **Query Params:** `district`, `category`, `min_rating`.
* **Response:** Danh sách các POI kèm tọa độ và rating.

## 5.2. Search & Enrichment API

* **Endpoint:** `/api/v1/enrich`
* **Method:** `POST`
* **Payload:** `{ "osm_id": "...", "osm_type": "..." }`
* **Action:** Gọi Google Places API để cập nhật thông tin mới nhất vào Lakehouse.

## 5.3. Analytic API

* **Endpoint:** `/api/v1/analytics/district-stats`
* **Method:** `GET`
* **Response:** Số liệu tổng hợp theo 30 Quận/Huyện phục vụ các Dashboard tùy biến.
