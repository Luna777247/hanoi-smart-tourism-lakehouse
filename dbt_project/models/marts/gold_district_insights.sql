{{ config(
    materialized='table',
    schema='gold'
) }}

WITH base_data AS (
    SELECT * FROM {{ ref('gold_attractions') }}
),

districts_mapped AS (
    SELECT
        *,
        CASE 
            WHEN lat BETWEEN 21.02 AND 21.04 AND lon BETWEEN 105.84 AND 105.86 THEN 'Hoan Kiem'
            WHEN lat BETWEEN 21.03 AND 21.05 AND lon BETWEEN 105.81 AND 105.83 THEN 'Ba Dinh'
            WHEN lat BETWEEN 21.05 AND 21.09 AND lon BETWEEN 105.80 AND 105.84 THEN 'Tay Ho'
            WHEN lat BETWEEN 21.00 AND 21.02 AND lon BETWEEN 105.83 AND 105.86 THEN 'Hai Ba Trung'
            WHEN lat BETWEEN 21.00 AND 21.03 AND lon BETWEEN 105.77 AND 105.81 THEN 'Cau Giay'
            ELSE 'Other Districts'
        END as district_name
    FROM base_data
),

district_summary AS (
    SELECT
        district_name,
        count(*) as attraction_count,
        avg(avg_rating) as avg_district_rating,
        sum(source_count) as total_interactions,
        count(DISTINCT category) as category_diversity
    FROM districts_mapped
    GROUP BY district_name
)

SELECT * FROM district_summary
ORDER BY attraction_count DESC
