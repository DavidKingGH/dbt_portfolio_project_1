SELECT
    -- Conversion Info
    c.purchase_id,
    c.user_id,
    c.purchase_timestamp,

    -- Touchpoint Info
    us.session_start_timestamp AS touchpoint_timestamp,
    us.session_source AS touchpoint_source,
    us.session_medium AS touchpoint_medium  

FROM
    {{ ref('stg__conversion_events') }} AS c
LEFT JOIN
    {{ ref('stg__user_sessions') }} AS us
    ON c.user_id = us.user_id 
        AND us.session_start_timestamp < c.purchase_timestamp
        AND us.session_start_timestamp >= c.purchase_timestamp - INTERVAL '30' DAY