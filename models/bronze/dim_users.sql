with source as (

select * from {{ source('bronze', 'dim_users') }}
), 

renamed_and_casted as (

select
    user_pseudo_id as user_id,
    make_timestamp(user_first_touch_timestamp::bigint) AS user_first_touch_timestamp
from source
)

select *
from renamed_and_casted