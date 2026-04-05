{{
  config(
    materialized  = 'table',
    file_format   = 'iceberg',
    schema        = 'gold',
    description   = 'Weekly average rating and review volume per district. Powers Superset line chart "Xu hướng rating theo thời gian".'
  )
}}

/*
  gold_rating_trend
  ──────────────────
  Weekly aggregation of rating & review metrics per district.
  Superset line chart: x-axis = week_start, y-axis = avg_rating / total_reviews.
*/

WITH weekly AS (
  SELECT
    -- Snap to Monday of each week for clean x-axis labels
    DATE_TRUNC('week', snapshot_date)            AS week_start,
    district,
    attraction_nk,
    AVG(rating)                                  AS weekly_avg_rating,
    MAX(user_ratings_total)                      AS peak_reviews_in_week,
    AVG(review_growth_rate)                      AS avg_growth_rate
  FROM {{ source('silver', 'fact_attraction_snapshot') }}
  WHERE district IS NOT NULL
    AND snapshot_date >= CURRENT_DATE - INTERVAL '{{ var("lookback_days") }}' DAY
  GROUP BY
    DATE_TRUNC('week', snapshot_date),
    district,
    attraction_nk
)

SELECT
  week_start,
  district,

  -- ── Rating metrics
  ROUND(AVG(weekly_avg_rating), 3)               AS avg_rating,
  ROUND(MAX(weekly_avg_rating), 3)               AS max_rating,

  -- ── Volume metrics
  COUNT(DISTINCT attraction_nk)                  AS active_attractions,
  SUM(peak_reviews_in_week)                      AS total_reviews,

  -- ── Growth
  ROUND(AVG(avg_growth_rate) * 100, 2)           AS avg_review_growth_rate_pct,

  CURRENT_TIMESTAMP                              AS refreshed_at

FROM weekly
GROUP BY week_start, district
ORDER BY week_start ASC, district ASC
