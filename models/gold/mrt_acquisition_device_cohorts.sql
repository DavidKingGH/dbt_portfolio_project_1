with

sessions as (

select
    user_id,
    ga_session_id,
    session_start_timestamp, 
    device_category
from {{ ref('stg__user_sessions') }}
where device_category is not null
),

ranked_sessions as (

select 
    user_id, 
    session_start_timestamp, 
    device_category,
    row_number() over(partition by user_id order by session_start_timestamp asc) as session_rank
from sessions
),

acquisition_cohorts as (

select
    user_id, 
    device_category as acquisition_device, 
    date_trunc('month', session_start_timestamp) as acquisition_month
from ranked_sessions
where session_rank = 1
), 

cohorts_user_orders as (

select
    uo.user_id,
    acquisition_device, 
    acquisition_month,
    date_trunc('month', order_date) as order_month,
    revenue    
from acquisition_cohorts ac
left join stg__user_orders uo 
    on ac.user_id =  uo.user_id
    where uo.order_date is not null
),

final as (

select 
     acquisition_device,
     acquisition_month,
     date_diff('month', acquisition_month, order_month) as months_since_acquisition,
     sum(revenue) as total_revenue, 
     count(*) as orders,
     count(distinct user_id) as purchasers
from cohorts_user_orders
group by 1,2,3
)

select * from final
order by acquisition_month, months_since_acquisition, acquisition_device