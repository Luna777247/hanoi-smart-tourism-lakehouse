{{ config(
    materialized='table'
) }}

WITH district_stats AS (
    SELECT
        snapshot_date,
        district_name,
        count(*) as poi_count,
        avg(avg_rating) as district_avg_rating,
        sum(popularity_score) as total_district_popularity,
        count(DISTINCT category) as category_diversity
    FROM {{ ref('gold_attractions') }}
    GROUP BY snapshot_date, district_name
)

SELECT * FROM district_stats
ORDER BY total_district_popularity DESC
