SELECT
    acq.session_source,
    acq.session_medium,
    COUNT(DISTINCT acq.user_id) AS acquired_customers,
    COALESCE(SUM(cr.total_revenue),0) AS total_purchase_value_from_cohort,
    COALESCE(AVG(cr.total_revenue),0) AS average_ltv_by_channel
FROM
    {{ ref('int__acquisition_touchpoints') }} AS acq
LEFT JOIN
    {{ref('int__customer_revenue')}} cr ON acq.user_id = cr.user_id
GROUP BY
    1, 2
ORDER BY
    average_ltv_by_channel DESC
