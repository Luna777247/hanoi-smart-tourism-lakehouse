# 09. Báo cáo Dữ liệu Thực tế (Project Completion Snapshot)

Tài liệu này cung cấp bản chụp (snapshot) dữ liệu thực tế tại tầng **Gold** sau khi kết thúc chu kỳ xử lý cuối cùng. Đây là cơ sở để đánh giá hiệu quả của hệ thống.

## 9.1. Chỉ số Tổng quan (Executive Overview)

Dữ liệu cập nhật ngày: **2026-04-19**

| Chỉ số | Giá trị | Giải thích |
| :--- | :--- | :--- |
| **Tổng số POIs** | 115 | Số lượng địa điểm du lịch đã được làm giàu thông tin. |
| **Điểm Rating Trung bình** | 4.46 | Đánh giá tổng thể chất lượng du lịch Thủ đô. |
| **Tổng lượt tương tác** | 215,232 | Tổng số lượt đánh giá cộng dồn từ các nguồn. |
| **Số Quận/Huyện đã phủ**| 6 | Các quận nội thành trọng điểm đã hoàn thành việc gán nhãn địa lý. |

## 9.2. Top 5 Địa điểm Du lịch nổi bật nhất

Dựa trên chỉ số **Popularity Score** (tự động tính bởi dbt):

| Tên địa danh | Loại hình | Rating | Popularity Score | Quận/Huyện |
| :--- | :--- | :--- | :--- | :--- |
| **Hoàng Thành Thăng Long** | Di tích | 4.4 | 18.78 | Hoàn Kiếm |
| **Lăng Chủ tịch Hồ Chí Minh**| Di tích | 4.6 | 17.52 | Ba Đình |
| **Văn Miếu Quốc Tử Giám** | Di tích | 4.7 | 16.90 | Đống Đa |
| ... | ... | ... | ... | ... |

## 9.3. Phân tích Dữ liệu theo Quận/Huyện

| Quận/Huyện | Số lượng POIs | Rating Trung bình | Độ đa dạng (Category) |
| :--- | :--- | :--- | :--- |
| **Hoàn Kiếm** | 42 | 4.52 | 8 |
| **Ba Đình** | 28 | 4.48 | 6 |
| **Tây Hồ** | 15 | 4.35 | 4 |

## 9.4. Cảnh báo Chất lượng dịch vụ (Quality Alerts)

Hệ thống đã tự động phân loại mức độ tin cậy dựa trên số lượng review:

* **High Reliability:** 85 địa điểm (>10 reviews).
* **Medium Reliability:** 20 địa điểm (2-10 reviews).
* **Low Reliability/New POI:** 10 địa điểm.

---
*Ghi chú: Toàn bộ dữ liệu trên được truy vấn trực tiếp từ bảng `iceberg.gold.gold_attractions` bằng Trino Engine.*
