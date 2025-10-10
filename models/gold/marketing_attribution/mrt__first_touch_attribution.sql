WITH all_touchpoints as (

SELECT *
from {{ ref('int__purchase_touchpoints') }}

),

valid_marketing_touchpoints as (

SELECT *
FROM all_touchpoints
WHERE
-- Exclude direct and non-tracked traffic
touchpoint_source IS NOT NULL 
AND touchpoint_source NOT IN ('(direct)', '<Other>', '(data deleted)')
AND touchpoint_medium IS NOT NULL
AND touchpoint_medium NOT IN ('(none)', '(not set)', 'referral', '<Other>', '(data deleted)', 'organic') 

)

ranked AS (
SELECT
*,
ROW_NUMBER() OVER (PARTITION BY purchase_id ORDER BY touchpoint_timestamp ASC) AS first_touch_rank
FROM valid_marketing_touchpoints

),

SELECT
  r.purchase_id::varchar as purchase_id,
  r.user_id::varchar as user_id,
  coalesce(c.purchase_revenue,0)::float as purchase_revenue,                       
  r.purchase_timestamp::timestamp as purchase_timestamp,
  r.touchpoint_timestamp::timestamp as touchpoint_timestamp,
  r.touchpoint_source::varchar as touchpoint_source,
  r.touchpoint_medium::varchar as touchpoint_medium
FROM ranked r
JOIN {{ ref('stg__conversion_events') }} c USING (purchase_id)
WHERE r.first_touch_rank = 1
