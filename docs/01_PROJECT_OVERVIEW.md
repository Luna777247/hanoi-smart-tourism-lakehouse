# 01. Tổng quan Dự án (Project Overview)

## 1.1. Giới thiệu

Dự án **Hanoi Smart Tourism Lakehouse** là hệ thống nền tảng dữ liệu hiện đại (Modern Data Stack) phục vụ việc thu thập, xử lý và phân tích dữ liệu du lịch thành phố Hà Nội. Hệ thống giúp chuyển đổi các dữ liệu thô từ bản đồ và mạng xã hội thành các chỉ số chiến lược hỗ trợ ra quyết định.

## 1.2. Mục tiêu Chiến lược

* **Số hóa hạ tầng du lịch:** Chuyển đổi thông tin từ OSM và Google thành kho dữ liệu có cấu trúc.
* **Nâng cao chất lượng dịch vụ:** Giám sát sự hài lòng của du khách thông qua điểm đánh giá (Rating) và nhận xét (Reviews).
* **Tối ưu hóa quy hoạch:** Sử dụng dữ liệu không gian để xác định các điểm nóng du lịch.

## 1.3. Các bên liên quan (Stakeholders)

1. **Sở Du lịch Hà Nội:** Đơn vị quản lý và khai thác chính.
2. **UBND các Quận/Huyện:** Theo dõi và phát triển du lịch địa phương.
3. **Doanh nghiệp Du lịch:** Tiếp cận dữ liệu xu hướng khách hàng.
4. **Du khách:** Hưởng lợi từ chất lượng dịch vụ được cải thiện.

## 1.4. Phạm vi dự án

* Thu thập dữ liệu từ OpenStreetMap (OSM) và Google Places.
* Xây dựng hệ thống Medallion Lakehouse (Bronze, Silver, Gold) trên nền tảng Apache Iceberg.
* Thiết lập 5 Dashboard phân tích chuyên sâu.
