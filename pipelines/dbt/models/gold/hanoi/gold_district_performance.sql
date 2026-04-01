{{
  config(
    materialized  = 'table',
    file_format   = 'iceberg',
    schema        = 'gold',
    description   = 'District-level KPIs – attraction count, avg rating, total reviews, share of total. Powers district bar/pie charts in Superset.'
  )
}}

/*
  gold_district_performance
  ──────────────────────────
  Aggregated by district from the latest snapshot.
  Used by Superset bar chart: "Phân bố điểm du lịch theo quận/huyện"
*/

WITH latest_snapshot AS (
  SELECT
    attraction_nk,
    district,
    attraction_type,
    rating,
    user_ratings_total,
    snapshot_date
  FROM {{ source('silver', 'fact_attraction_snapshot') }}
  WHERE snapshot_date = (
    SELECT MAX(snapshot_date)
    FROM {{ source('silver', 'fact_attraction_snapshot') }}
  )
  AND district IS NOT NULL
),

city_totals AS (
  SELECT
    SUM(user_ratings_total)  AS city_total_reviews,
    COUNT(DISTINCT attraction_nk) AS city_total_attractions
  FROM latest_snapshot
)

SELECT
  ls.district,

  -- ── Volume
  COUNT(DISTINCT ls.attraction_nk)                              AS attraction_count,
  SUM(ls.user_ratings_total)                                    AS total_reviews,

  -- ── Quality
  ROUND(AVG(ls.rating), 2)                                      AS avg_rating,
  MAX(ls.rating)                                                AS max_rating,
  MIN(ls.rating)                                                AS min_rating,

  -- ── Share of city
  ROUND(
    100.0 * COUNT(DISTINCT ls.attraction_nk) / ct.city_total_attractions, 1
  )                                                             AS pct_of_attractions,
  ROUND(
    100.0 * SUM(ls.user_ratings_total) / NULLIF(ct.city_total_reviews, 0), 1
  )                                                             AS pct_of_reviews,

  -- ── Breakdown by type
  COUNT(DISTINCT CASE WHEN ls.attraction_type = 'Bảo tàng'    THEN ls.attraction_nk END) AS cnt_museum,
  COUNT(DISTINCT CASE WHEN ls.attraction_type = 'Đền / Chùa'  THEN ls.attraction_nk END) AS cnt_temple,
  COUNT(DISTINCT CASE WHEN ls.attraction_type = 'Công viên'   THEN ls.attraction_nk END) AS cnt_park,
  COUNT(DISTINCT CASE WHEN ls.attraction_type = 'Điểm tham quan' THEN ls.attraction_nk END) AS cnt_attraction,

  ls.snapshot_date,
  CURRENT_TIMESTAMP                                             AS refreshed_at

FROM latest_snapshot   ls
CROSS JOIN city_totals ct
GROUP BY ls.district, ls.snapshot_date, ct.city_total_reviews, ct.city_total_attractions
ORDER BY total_reviews DESC
