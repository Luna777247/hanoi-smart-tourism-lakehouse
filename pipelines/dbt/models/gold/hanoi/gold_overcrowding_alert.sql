{{
  config(
    materialized  = 'table',
    file_format   = 'iceberg',
    schema        = 'gold',
    description   = 'Overcrowding risk assessment per attraction. HIGH risk = popular + fast-growing reviews. Powers Superset alert table.'
  )
}}

/*
  gold_overcrowding_alert
  ────────────────────────
  Identifies attractions at risk of overcrowding using a rule-based scoring model.

  Risk Logic (configurable via dbt vars):
  ┌─────────────┬────────────────────────────────────────────────────────┐
  │ Level       │ Condition                                              │
  ├─────────────┼────────────────────────────────────────────────────────┤
  │ HIGH        │ rating ≥ threshold AND review_growth ≥ growth_thresh   │
  │             │ AND is_operational AND reviews ≥ min_reviews           │
  │ MEDIUM      │ rating ≥ threshold AND reviews ≥ min_reviews           │
  │             │ (popular but not growing fast)                         │
  │ WATCH       │ review_growth ≥ growth_thresh (growing but not yet     │
  │             │ highly rated)                                          │
  │ NORMAL      │ everything else                                        │
  └─────────────┴────────────────────────────────────────────────────────┘

  Thresholds (dbt vars, overridable at runtime):
    overcrowding_review_growth_threshold : 0.30  (30% 30-day growth)
    overcrowding_min_rating              : 4.30
    overcrowding_min_reviews             : 500
*/

WITH summary AS (
  SELECT *
  FROM {{ ref('gold_attractions_summary') }}
  WHERE is_operational = true
    AND current_reviews IS NOT NULL
),

scored AS (
  SELECT
    *,

    -- ── Individual signal flags
    (current_rating        >= {{ var('overcrowding_min_rating') }})   AS flag_high_rating,
    (current_reviews       >= {{ var('overcrowding_min_reviews') }})  AS flag_high_volume,
    (avg_daily_growth_rate_30d * 30 >= {{ var('overcrowding_review_growth_threshold') }})
                                                                       AS flag_fast_growth,

    -- ── Composite risk score (0–3)
    CAST(current_rating >= {{ var('overcrowding_min_rating') }}  AS INT)
    + CAST(current_reviews >= {{ var('overcrowding_min_reviews') }} AS INT)
    + CAST(avg_daily_growth_rate_30d * 30 >= {{ var('overcrowding_review_growth_threshold') }} AS INT)
      AS risk_score
  FROM summary
),

classified AS (
  SELECT
    *,
    CASE
      WHEN flag_high_rating AND flag_high_volume AND flag_fast_growth THEN 'HIGH'
      WHEN flag_high_rating AND flag_high_volume                      THEN 'MEDIUM'
      WHEN flag_fast_growth                                           THEN 'WATCH'
      ELSE                                                                 'NORMAL'
    END AS risk_level,

    CASE
      WHEN flag_high_rating AND flag_high_volume AND flag_fast_growth
        THEN 'Điểm tham quan nổi tiếng với lượng review tăng nhanh – cần điều tiết dòng khách.'
      WHEN flag_high_rating AND flag_high_volume
        THEN 'Điểm được đánh giá cao và đông khách – theo dõi chặt chẽ.'
      WHEN flag_fast_growth
        THEN 'Review tăng đột biến – có thể trở thành điểm nóng trong thời gian tới.'
      ELSE 'Bình thường.'
    END AS risk_note
  FROM scored
)

SELECT
  -- ── Identity
  attraction_sk,
  place_id,
  name,
  district,
  attraction_type,
  lat,
  lng,
  formatted_address,

  -- ── Metrics used for scoring
  current_rating,
  current_reviews,
  ROUND(avg_daily_growth_rate_30d * 30 * 100, 1) AS review_growth_30d_pct,
  review_growth_30d,
  avg_rating_30d,
  rating_vs_30d_avg,

  -- ── Risk output
  risk_level,
  risk_score,
  risk_note,
  flag_high_rating,
  flag_high_volume,
  flag_fast_growth,

  -- ── Metadata
  last_snapshot_date,
  refreshed_at

FROM classified
ORDER BY
  CASE risk_level
    WHEN 'HIGH'   THEN 1
    WHEN 'MEDIUM' THEN 2
    WHEN 'WATCH'  THEN 3
    ELSE               4
  END,
  risk_score DESC,
  current_reviews DESC
