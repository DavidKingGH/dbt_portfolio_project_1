
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
        count(*) as orders_on_device, 
        sum(coalesce(revenue,0)) as total_revenue,
        count(distinct ga_session_id) as total_sessions 
    from base
    group by 1,2

), 

user_devices_ranked as (

    select
        user_id,
        device_category,
        orders_on_device,
        total_revenue,
        total_sessions,
        row_number() over(partition by user_id order by orders_on_device desc, total_revenue desc, total_sessions desc, device_category asc) as rank_num
    from user_total_device_sessions
),

quartile_user_top_devices as (

    select
        user_id,
        device_category as dominant_device,
        total_revenue,
        total_sessions,
        ntile(4) over(partition by device_category order by orders_on_device desc, total_revenue DESC, total_sessions desc, device_category asc) as spend_quartile
    from user_devices_ranked
    where rank_num = 1
)


select
    user_id::varchar as user_id,
    dominant_device::varchar as dominant_device,
    total_revenue::decimal as total_revenue,
    spend_quartile::int as spend_quartile,
    coalesce(total_sessions,0)::int as total_sessions 
    dominant_device || ' - Q' || spend_quartile ||
        case
            when spend_quartile = 1 THEN ' (Top Spenders)'
            when spend_quartile = 4 THEN ' (Low Spenders)'
            else ''
        end as segment_name::varchar as segment_name
from quartile_user_top_devices
