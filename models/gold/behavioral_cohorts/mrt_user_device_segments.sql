
WITH
base as (
    SELECT
        user_id, 
        device_category,
        revenue,
        ga_session_id
    from {{ ref('stg__user_orders')}}
),

user_total_device_sessions as (

    select
        user_id, 
        device_category,
        count(device_category) as device_used, 
        sum(revenue) as total_revenue,
        count(distinct ga_session_id) as total_sessions 
    from base
    group by 1,2

), 

user_devices_ranked as (

    select
        user_id,
        device_category,
        total_revenue,
        total_sessions,
        row_number() over(partition by user_id order by device_used desc) as rank_num
    from user_total_device_sessions
),

quartile_user_top_devices as (

    select
        user_id,
        device_category as dominant_device,
        total_revenue,
        total_sessions,
        ntile(4) over(partition by device_category order by total_revenue DESC) as cart_value_quartile
    from user_devices_ranked
    where rank_num = 1
)


select
    user_id,
    dominant_device,
    total_revenue,
    cart_value_quartile, 
    dominant_device || ' - Q' || cart_value_quartile ||
        case
            when cart_value_quartile = 1 THEN ' (Top Spenders)'
            when cart_value_quartile = 4 THEN ' (Low Spenders)'
            else ''
        end as segment_name
from quartile_user_top_devices
order by dominant_device, cart_value_quartile
