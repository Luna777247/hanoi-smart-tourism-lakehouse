{{
  config(
    materialized  = 'table',
    file_format   = 'iceberg',
    schema        = 'gold',
    description   = 'One row per attraction – latest snapshot enriched with 30-day trend metrics. Primary source for KPI cards and top-N charts.'
  )
}}

/*
  gold_attractions_summary
  ─────────────────────────
  Aggregates the latest snapshot per attraction with trailing 30-day metrics.
  Used by Superset for:
    • KPI cards  (total attractions, avg rating, total reviews)
    • Top-10 highest-rated table
    • Map chart  (lat / lng pins)
*/

WITH latest AS (
  -- Most recent daily snapshot per attraction
  SELECT *
  FROM {{ source('silver', 'fact_attraction_snapshot') }}
  WHERE snapshot_date = (
    SELECT MAX(snapshot_date)
    FROM {{ source('silver', 'fact_attraction_snapshot') }}
  )
),

trailing_30d AS (
  -- 30-day aggregates for trend context
  SELECT
    attraction_nk,
    COUNT(DISTINCT snapshot_date)                        AS active_days_30d,
    AVG(rating)                                          AS avg_rating_30d,
    MAX(user_ratings_total)                              AS max_reviews_30d,
    MIN(user_ratings_total)                              AS min_reviews_30d,
    MAX(user_ratings_total) - MIN(user_ratings_total)    AS review_growth_30d,
    AVG(review_growth_rate)                              AS avg_daily_growth_rate_30d
  FROM {{ source('silver', 'fact_attraction_snapshot') }}
  WHERE snapshot_date >= CURRENT_DATE - INTERVAL '{{ var("lookback_days") }}' DAY
  GROUP BY attraction_nk
),

dim AS (
  SELECT *
  FROM {{ source('silver', 'dim_attraction') }}
  WHERE attraction_nk IS NOT NULL
)

SELECT
  -- ── Keys
  d.attraction_sk,
  d.attraction_nk                                        AS place_id,

  -- ── Descriptive attributes
  d.name,
  d.formatted_address,
  d.district,
  d.attraction_type,
  d.lat,
  d.lng,
  d.price_level,
  d.is_operational,
  d.business_status,
  d.opening_hours_open_now,
  d.vicinity,

  -- ── Latest snapshot metrics
  l.rating                                               AS current_rating,
  l.user_ratings_total                                   AS current_reviews,
  l.snapshot_date                                        AS last_snapshot_date,

  -- ── 30-day trend
  t.avg_rating_30d,
  t.review_growth_30d,
  t.avg_daily_growth_rate_30d,
  t.active_days_30d,

  -- ── Derived
  ROUND(l.rating - t.avg_rating_30d, 2)                  AS rating_vs_30d_avg,
  CURRENT_TIMESTAMP                                      AS refreshed_at

FROM dim          d
JOIN latest       l ON d.attraction_nk = l.attraction_nk
LEFT JOIN trailing_30d t ON d.attraction_nk = t.attraction_nk
