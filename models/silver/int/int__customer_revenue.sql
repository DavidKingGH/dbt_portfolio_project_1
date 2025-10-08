
WITH customer_purchase_revenue AS (
    SELECT
        user_id,
        SUM(purchase_revenue) AS total_revenue
    FROM
        {{ ref('stg__conversion_events') }}
    GROUP BY
        1
)

select *
from customer_purchase_revenue

