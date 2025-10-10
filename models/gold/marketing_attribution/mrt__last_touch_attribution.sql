WITH ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY purchase_id ORDER BY touchpoint_timestamp DESC) AS last_touch_rank
  FROM {{ ref('int__purchase_touchpoints') }}
)

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
WHERE r.last_touch_rank = 1
