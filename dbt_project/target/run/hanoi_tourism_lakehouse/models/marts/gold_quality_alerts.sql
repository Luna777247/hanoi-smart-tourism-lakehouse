
  
    

    create table "iceberg"."gold"."gold_quality_alerts__dbt_tmp"
      
      
    as (
      

WITH current_data AS (
    SELECT * FROM "iceberg"."gold"."gold_attractions"
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
    );

  