# 03. Từ điển Dữ liệu (Data Dictionary)

Tài liệu này mô tả các thực thể dữ liệu chính trong tầng **Gold** và các công thức tính toán chỉ số.

## 3.1. Thực thể chính: `gold_attractions`

Đây là bảng master chứa thông tin chuẩn hóa của tất cả địa danh.

| Trường | Kiểu dữ liệu | Mô tả |
| :--- | :--- | :--- |
| `attraction_key` | `varchar` | Khóa chính (MD5 hash của name + coordinates). |
| `name` | `varchar` | Tên địa danh (đã chuẩn hóa Initcap). |
| `category` | `varchar` | Loại hình du lịch chính. |
| `avg_rating` | `double` | Điểm đánh giá trung bình từ Google. |
| `source_count` | `bigint` | Tổng số lượt review hoặc nguồn dữ liệu. |
| `popularity_score`| `double` | Chỉ số độ nổi tiếng: `rating * log10(source_count + 1)`. |
| `district_name` | `varchar` | Tên Quận/Huyện (ánh xạ từ tọa độ). |
| `snapshot_date` | `date` | Ngày ghi nhận dữ liệu (Chiều thời gian). |

## 3.2. Quy tắc phân vùng địa lý (District Mapping)

Hệ thống tự động phân loại địa điểm vào các quận dựa trên Bound Box:

* **Hoàn Kiếm:** Lat[21.02, 21.04], Lon[105.84, 105.86]
* **Ba Đình:** Lat[21.03, 21.05], Lon[105.81, 105.83]
* (Và các quận khác tương ứng...)

## 3.3. Chất lượng dữ liệu (Data Quality Rules)

* **Unique:** `attraction_key` phải là duy nhất trong cùng một `snapshot_date`.
* **Not Null:** Tên và tọa độ không được để trống.
* **Range:** `avg_rating` phải nằm trong khoảng [0, 5].
