-- tests/assert_overcrowding_risk_levels_present.sql
-- Warns if zero HIGH-risk attractions exist despite having >= 50 operational
-- attractions with enough data. This catches silent failures in the scoring logic.
-- Severity: warn (not error) – configured in dbt_project.yml.

WITH counts AS (
  SELECT
    COUNT(*) FILTER (WHERE risk_level = 'HIGH')   AS high_count,
    COUNT(*) FILTER (WHERE risk_level != 'NORMAL') AS non_normal_count,
    COUNT(*)                                        AS total
  FROM {{ ref('gold_overcrowding_alert') }}
)

SELECT
  high_count,
  non_normal_count,
  total,
  'Expected at least 1 HIGH-risk attraction when total >= 50' AS failure_reason
FROM counts
WHERE total >= 50
  AND high_count = 0
  AND non_normal_count = 0
