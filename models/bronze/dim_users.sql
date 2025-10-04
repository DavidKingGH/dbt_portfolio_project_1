SELECT *
FROM {{ source('ga4_ecommerce_data', 'dim_users')}}