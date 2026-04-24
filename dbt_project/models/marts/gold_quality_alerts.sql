{{ config(
    materialized='table'
) }}

WITH current_data AS (
    SELECT * FROM {{ ref('gold_attractions') }}
),

alerts AS (
    SELECT
        snapshot_date,
        name,
        category,
        avg_rating,
        reliability_level,
        CASE 
            WHEN avg_rating < 3.0 THEN 'CRITICAL'
            WHEN avg_rating < 4.0 THEN 'WARNING'
            ELSE 'HEALTHY'
        END as health_status
    FROM current_data
)

SELECT * FROM alerts
WHERE health_status IN ('CRITICAL', 'WARNING')
ORDER BY avg_rating ASC
