-- File: dbt_project/models/marts/gold_attractions.sql
{{ config(
    materialized='table'
) }}

WITH reference_districts AS (
    -- Dữ liệu tọa độ 30 Quận/Huyện/Thị xã do người dùng cung cấp
    SELECT * FROM (VALUES
        ('Ba Dinh', 21.0366, 105.8290),
        ('Hoan Kiem', 21.0287, 105.8524),
        ('Dong Da', 21.0131, 105.8262),
        ('Hai Ba Trung', 21.0125, 105.8546),
        ('Cau Giay', 21.0357, 105.7958),
        ('Thanh Xuan', 20.9941, 105.8078),
        ('Tay Ho', 21.0725, 105.8211),
        ('Hoang Mai', 20.9639, 105.8572),
        ('Long Bien', 21.0425, 105.9011),
        ('Ha Dong', 20.9630, 105.7640),
        ('Bac Từ Liêm', 21.0631, 105.7535),
        ('Nam Từ Liêm', 21.0185, 105.7628),
        ('Son Tay', 21.1394, 105.5039),
        ('Ba Vi', 21.2183, 105.4111),
        ('Chuong My', 20.8806, 105.6792),
        ('Dan Phuong', 21.1072, 105.6717),
        ('Dong Anh', 21.1442, 105.8503),
        ('Gia Lam', 21.0225, 105.9406),
        ('Hoai Duc', 21.0203, 105.7058),
        ('Me Linh', 21.1925, 105.7364),
        ('My Duc', 20.6975, 105.7442),
        ('Phu Xuyen', 20.7289, 105.9125),
        ('Phuc Tho', 21.1086, 105.5794),
        ('Quoc Oai', 20.9903, 105.6417),
        ('Soc Son', 21.2581, 105.8500),
        ('Thach That', 21.0064, 105.5564),
        ('Thanh Oai', 20.8753, 105.7797),
        ('Thanh Tri', 20.9500, 105.8500),
        ('Thuong Tin', 20.8367, 105.8550),
        ('Ung Hoa', 20.7264, 105.8039)
    ) AS t (district_name, center_lat, center_lon)
),

silver_data AS (
    SELECT 
        name,
        poi_types[1] as category,
        latitude as lat,
        longitude as lon,
        rating,
        review_count as source_count,
        'google_osm' as sources
    FROM {{ source('silver', 'attractions_enriched') }}
),

calc_distances AS (
    SELECT 
        s.*,
        r.district_name,
        -- Tính khoảng cách Euclidean bình phương (đủ để so sánh gần nhất)
        ((s.lat - r.center_lat) * (s.lat - r.center_lat) + (s.lon - r.center_lon) * (s.lon - r.center_lon)) as distance_sq,
        ROW_NUMBER() OVER (PARTITION BY s.name, s.lat, s.lon ORDER BY ((s.lat - r.center_lat) * (s.lat - r.center_lat) + (s.lon - r.center_lon) * (s.lon - r.center_lon)) ASC) as rank_nearest
    FROM silver_data s
    CROSS JOIN reference_districts r
),

final AS (
    SELECT
        md5(to_utf8(name || CAST(lat AS VARCHAR) || CAST(lon AS VARCHAR))) as attraction_key,
        name,
        category,
        lat,
        lon,
        rating as avg_rating,
        source_count,
        sources,
        CASE 
            WHEN source_count >= 3 THEN 'High'
            WHEN source_count = 2 THEN 'Medium'
            ELSE 'Low'
        END as reliability_level,
        rating * log10(source_count + 1) as popularity_score,
        district_name,
        current_date as snapshot_date,
        current_timestamp as load_timestamp
    FROM calc_distances
    WHERE rank_nearest = 1
)

SELECT * FROM final
