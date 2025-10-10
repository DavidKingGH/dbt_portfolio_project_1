SELECT
    -- Conversion Info
    c.purchase_id,
    c.user_id,
    c.purchase_timestamp,

    -- Touchpoint Info
    t.touchpoint_timestamp,
    t.touchpoint_source,
    t.touchpoint_medium
FROM
    {{ ref('stg__conversion_events') }} AS c
LEFT JOIN
    {{ ref('int__campaign_touchpoints') }} AS t
    ON c.user_id = t.user_id
    and t.touchpoint_timestamp between c.purchase_timestamp - interval '30' day and c.purchase_timestamp