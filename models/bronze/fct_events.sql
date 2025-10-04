SELECT *
FROM {{ source('ga4_ecommerce_data', 'fact_events')}}