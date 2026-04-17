# File: dbt_project/models/marts/gold_attractions.sql
{{ config(
    materialized='table',
    schema='gold'
) }}

WITH silver_data AS (
    SELECT * FROM {{ source('silver', 'unified_attractions') }}
),

final AS (
    SELECT
        md5(name || lat || lon) as attraction_key,
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
        current_timestamp() as load_timestamp
    FROM silver_data
    WHERE name IS NOT NULL
)

SELECT * FROM final
