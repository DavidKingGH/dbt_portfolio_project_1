WITH ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY purchase_id ORDER BY touchpoint_timestamp DESC) AS last_touch_rank
  FROM {{ ref('int_user_journey') }} 
)

SELECT
  r.purchase_id,
  r.user_pseudo_id,
  c.purchase_revenue,                       
  r.purchase_timestamp,
  r.touchpoint_timestamp,
  r.touchpoint_source,
  r.touchpoint_medium
FROM ranked r
JOIN {{ ref('stg_conversion_events') }} c USING (purchase_id)
WHERE r.last_touch_rank = 1
