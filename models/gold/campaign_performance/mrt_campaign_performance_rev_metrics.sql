WITH sessions_with_revenue AS (
    SELECT
        s.ga_session_id,
        s.user_id,
        s.session_source,
        s.session_medium,
        c.purchase_revenue -- NULL when there is no revenue
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
    SUM(purchase_revenue) AS total_revenue,
    COUNT(DISTINCT user_id) AS total_users,
    COUNT(DISTINCT CASE WHEN purchase_revenue IS NOT NULL THEN user_id END) AS converting_users,
    total_revenue / NULLIF(COUNT(DISTINCT CASE WHEN purchase_revenue IS NOT NULL THEN ga_session_id END), 0) AS average_order_value
FROM
    sessions_with_revenue
GROUP BY
    1, 2