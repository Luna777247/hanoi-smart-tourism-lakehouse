# 04. Đặc tả Dashboard (Dashboard Specification)

Tài liệu này hướng dẫn cách xây dựng 5 Dashboard chiến lược trên Apache Superset.

## 4.1. Dashboard 1: Tổng quan (Executive Health)

* **KPIs:** Total POIs, City Avg Rating, Total Interactions.
* **Visuals:** Big Number Chart, Timeline Trends.

## 4.2. Dashboard 2: Chất lượng & Cảnh báo (Quality Alerts)

* **KPIs:** % High Rating, Rating Delta.
* **Visuals:** Leaderboard Table (Top 10), Bar chart (Rating by Category).
* **Alert Logic:** Hiển thị màu đỏ nếu Rating < 3.0.

## 4.3. Dashboard 3: Bản đồ Nhiệt (Heatmap)

* **KPIs:** Popularity Score.
* **Visuals:** Heatmap Layer (Folium/Kepler.gl).
* **Filter:** Dynamic date slider.

## 4.4. Dashboard 4: So sánh Quận/Huyện (District Benchmarking)

* **KPIs:** Rank by District, Diversity Score.
* **Visuals:** Radar Chart, Clustered Bar Chart.

## 4.5. Dashboard 5: Hạ tầng & Thông tin (Service Accessibility)

* **KPIs:** Website availability %, Weekend opening %.
* **Visuals:** Sunburst Chart, Pie Chart.
