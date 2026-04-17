# Google Map Places API Specification (RapidAPI)

Tài liệu này tổng hợp các thông tin kỹ thuật, cấu hình và bảng giá của Google Map Places API được sử dụng trong dự án Hanoi Smart Tourism Lakehouse.

---

## 1. Thông tin chung

- **Nhà cung cấp**: Google Map Platform (qua RapidAPI)
- **Host**: `google-map-places.p.rapidapi.com`
- **URL cơ bản**: `https://google-map-places.p.rapidapi.com/maps/api/place/`

---

## 2. Bảng giá & Định mức (Pricing Plans)

Hệ thống RapidAPI áp dụng chính sách **Hard Limit** cho các lượt truy cập vượt định mức.

| Gói (Plan) | Chi phí/Tháng | Lượt yêu cầu (Requests) | Rate Limit (Tốc độ) | Phí băng thông |
| :--- | :--- | :--- | :--- | :--- |
| **Basic** | $0.00 | 100 / Ngày | 1000 requests/hour | 10GB miễn phí |
| **Pro** | $9.99 | 20,000 / Tháng | 5 requests/second | 10GB miễn phí |
| **Ultra** | $29.00 | 75,000 / Tháng | 10 requests/second | 10GB miễn phí |
| **Mega** | $299.00 | 2,000,000 / Tháng | 50 requests/second | 10GB miễn phí |

---

## 3. Danh sách Endpoints quan trọng

| Endpoint | Method | Mô tả |
| :--- | :--- | :--- |
| `findPlaceFromText` | GET | Tìm địa điểm dựa trên tên hoặc số điện thoại. |
| `textSearch` | GET | Tìm kiếm chi tiết dựa trên chuỗi văn bản (Query). |
| `nearbySearch` | GET | Tìm kiếm các địa điểm xung quanh một tọa độ GPS. |
| `placeDetails` | GET | Lấy thông tin chi tiết của một địa điểm cụ thể qua `place_id`. |
| `autocomplete` | GET | Gợi ý địa điểm khi người dùng đang nhập văn bản. |
| `placePhoto` | GET | Truy xuất hình ảnh của địa điểm. |

---

## 4. Tham số truy vấn (Parameters) cho `findPlaceFromText`

| Tham số | Bắt buộc | Kiểu | Mô tả |
| :--- | :--- | :--- | :--- |
| `input` | Có | String | Tên địa điểm, địa chỉ hoặc loại hình (VD: "Văn Miếu"). |
| `inputtype` | Có | String | Kiểu đầu vào: `textquery` hoặc `phonenumber`. |
| `fields` | Không | String | Danh sách các trường dữ liệu cần trả về (phân cách bằng dấu phẩy). |
| `locationbias` | Không | String | Ưu tiên kết quả trong khu vực nhất định (Radius/Rectangle). |
| `language` | Không | String | Mã ngôn ngữ (VD: `vi`, `en`). Mặc định là `en`. |

---

## 5. Phân loại Trường dữ liệu (Field Categories & Billing)

Việc chọn đúng `fields` giúp tối ưu chi phí vì Google tính phí khác nhau cho từng nhóm dữ liệu.

### A. Nhóm Cơ bản (Basic - Base Rate)

*Các trường này thường đi kèm trong mọi gói cước.*

- `name`, `formatted_address`, `geometry`, `place_id`, `type`, `url`, `utc_offset`, `vicinity`, `plus_code`, `icon`.

### B. Nhóm Liên hệ (Contact - Higher Rate)

- `website`, `formatted_phone_number`, `international_phone_number`, `opening_hours`.

### C. Nhóm Không khí/Đánh giá (Atmosphere - Premium Rate)

- `rating`, `user_ratings_total`, `reviews`, `price_level`, `editorial_summary`, `curbside_pickup`, `delivery`.

---

## 6. Mã nguồn mẫu (Python Implementation)

```python
import requests

url = "https://google-map-places.p.rapidapi.com/maps/api/place/findplacefromtext/json"

# Cấu hình tham số
querystring = {
    "input": "Museum of Contemporary Art Australia",
    "inputtype": "textquery",
    "fields": "all",
    "language": "en"
}

# Cấu hình Authentication
headers = {
    "x-rapidapi-key": "YOUR_API_KEY_HERE",
    "x-rapidapi-host": "google-map-places.p.rapidapi.com",
    "Content-Type": "application/json"
}

# Thực thi Request
response = requests.get(url, headers=headers, params=querystring)

if response.status_code == 200:
    data = response.json()
    print(data)
else:
    print(f"Error: {response.status_code}")
```

---
*Ghi chú: Luôn quản lý API Key thông qua HashiCorp Vault hoặc biến môi trường trong dự án.*
