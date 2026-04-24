
  
    

    create table "iceberg"."gold"."gold_tourism_heatmap__dbt_tmp"
      
      
    as (
      

SELECT
    snapshot_date,
    name,
    category,
    lat,
    lon,
    avg_rating,
    popularity_score,
    district_name
FROM "iceberg"."gold"."gold_attractions"
WHERE lat IS NOT NULL AND lon IS NOT NULL
    );

  