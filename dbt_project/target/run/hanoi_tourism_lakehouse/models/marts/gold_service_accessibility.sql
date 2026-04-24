
  
    

    create table "iceberg"."gold"."gold_service_accessibility__dbt_tmp"
      
      
    as (
      

SELECT
    snapshot_date,
    category,
    district_name,
    reliability_level,
    count(*) as total_locations,
    sum(CASE WHEN reliability_level = 'High' THEN 1 ELSE 0 END) as verified_count,
    avg(popularity_score) as avg_popularity
FROM "iceberg"."gold"."gold_attractions"
GROUP BY snapshot_date, category, district_name, reliability_level
    );

  