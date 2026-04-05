{{ config(
    materialized='table',
    table_type='iceberg',
    schema='gold'
) }}

WITH stg_google AS (
    SELECT
        place_id AS source_id,
        name,
        lat,
        lng,
        rating,
        user_ratings_total AS reviews_count,
        formatted_address AS address,
        'Google' AS source_system,
        ingested_at
    FROM {{ source('bronze_hanoi', 'hanoi_attractions') }}
),

stg_tripadvisor AS (
    SELECT
        place_id AS source_id,
        title AS name,
        NULL AS lat, -- Tripadvisor API qua SerpApi đôi khi không có tọa độ trực tiếp
        NULL AS lng,
        rating,
        reviews_count,
        location_string AS address,
        'Tripadvisor' AS source_system,
        collected_at AS ingested_at
    FROM {{ source('bronze_hanoi', 'hanoi_tripadvisor') }}
),

stg_osm AS (
    SELECT
        CAST(osm_id AS STRING) AS source_id,
        name,
        lat,
        lon AS lng,
        NULL AS rating, -- OSM thường không có rating
        NULL AS reviews_count,
        display_name AS address,
        'OSM' AS source_system,
        collected_at AS ingested_at
    FROM {{ source('bronze_hanoi', 'hanoi_osm') }}
),

unified_attractions AS (
    SELECT * FROM stg_google
    UNION ALL
    SELECT * FROM stg_tripadvisor
    UNION ALL
    SELECT * FROM stg_osm
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY LOWER(TRIM(name)) -- Khử trùng theo tên (không phân biệt hoa thường)
            ORDER BY rating DESC NULLS LAST, ingested_at DESC
        ) as rank
    FROM unified_attractions
    WHERE name IS NOT NULL
)

SELECT
    source_id,
    name,
    lat,
    lng,
    rating,
    reviews_count,
    address,
    source_system,
    ingested_at,
    CURRENT_TIMESTAMP() as processed_at
FROM deduplicated
WHERE rank = 1
