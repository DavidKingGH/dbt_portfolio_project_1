WITH sessions_with_revenue AS (
    SELECT
        s.ga_session_id,
        s.user_id,
        -- Handle missing/placeholder values
        COALESCE(NULLIF(TRIM(s.session_source), ''), '(not set)') AS session_source,
        COALESCE(NULLIF(TRIM(s.session_medium), ''), '(not set)') AS session_medium,
        c.purchase_revenue
    FROM
        {{ ref('stg__user_sessions') }} AS s
    LEFT JOIN
        {{ ref('stg__conversion_events') }} AS c
        ON s.ga_session_id = c.ga_session_id
)

-- Aggregate to the channel level
SELECT
    session_source,
    session_medium,
    COALESCE(SUM(purchase_revenue), 0) AS total_revenue,
    COUNT(DISTINCT user_id) AS total_users,
    COUNT(DISTINCT CASE WHEN purchase_revenue IS NOT NULL THEN user_id END) AS converting_users,
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN purchase_revenue IS NOT NULL THEN ga_session_id END) > 0 
        THEN SUM(purchase_revenue) / COUNT(DISTINCT CASE WHEN purchase_revenue IS NOT NULL THEN ga_session_id END)
        ELSE 0 
    END AS average_order_value
FROM
    sessions_with_revenue
GROUP BY
    session_source,
    session_medium