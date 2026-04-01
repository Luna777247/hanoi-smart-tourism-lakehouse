-- tests/assert_no_missing_district.sql
-- Fails if any OPERATIONAL attraction has district = 'Không xác định'
-- AND has >= 100 reviews (i.e. well-known enough to have a findable address).
--
-- dbt test --select hanoi_tourism

SELECT
  place_id,
  name,
  formatted_address,
  district,
  current_reviews
FROM {{ ref('gold_attractions_summary') }}
WHERE is_operational = true
  AND district = 'Không xác định'
  AND current_reviews >= 100
