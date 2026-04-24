{{ config(
    materialized='table'
) }}

SELECT
    snapshot_date,
    name,
    category,
    lat,
    lon,
    avg_rating,
    popularity_score,
    district_name
FROM {{ ref('gold_attractions') }}
WHERE lat IS NOT NULL AND lon IS NOT NULL
