{{ config(
    materialized='table'
) }}

WITH source_data AS (
    SELECT * FROM {{ ref('gold_attractions') }}
)

SELECT
    snapshot_date,
    count(*) as total_pois,
    avg(avg_rating) as city_avg_rating,
    sum(source_count) as total_interactions,
    count(DISTINCT district_name) as districts_covered
FROM source_data
GROUP BY snapshot_date
