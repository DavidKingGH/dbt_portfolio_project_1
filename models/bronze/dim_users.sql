with source as (
SELECT *
-- FROM {{ source('ga4_ecommerce_data', 'dim_users')}}
FROM {{ get_parquet_path('ga4_ecommerce_data', 'dim_users')}}

), 

renamed_and_casted as (

select
    user_pseudo_id as user_id,
    make_timestamp(user_first_touch_timestamp::bigint) AS user_first_touch_timestamp,
  	loaded_at
from source
)

select *
from renamed_and_casted