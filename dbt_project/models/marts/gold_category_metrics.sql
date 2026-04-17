{{ config(
    materialized='table',
    schema='gold'
) }}

WITH source_data AS (
    SELECT * FROM {{ ref('gold_attractions') }}
),

category_metrics AS (
    SELECT
        category,
        count(*) as total_locations,
        avg(avg_rating) as average_category_rating,
        sum(CASE WHEN reliability_level = 'High' THEN 1 ELSE 0 END) as verified_locations,
        max(avg_rating) as top_rating
    FROM source_data
    GROUP BY category
)

SELECT 
    *,
    (verified_locations * 1.0 / total_locations) * 100 as trust_percentage
FROM category_metrics
ORDER BY average_category_rating DESC
