with

user_sessions as (

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
    us.user_id,
    acquisition_device, 
    acquisition_month,
    date_trunc('month', order_date) as order_month,
    revenue    
from acquisition_cohorts ac
left join user_sessions us 
    on ac.user_id =  us.user_id
    and us.order_date is not null
    and uo.order_date >= ac.acquisition_month
),

final as (

select 
     acquisition_device::varchar as acquisition_device,
     acquisition_month::date as acquisition_month,
     date_diff('month', acquisition_month, order_month)::int as months_since_acquisition,
     sum(coalesce(revenue,0))::decimal as total_revenue, 
     count(case when revenue is not null then 1 end)::int as orders,
     count(distinct case when revenue is not null then user_id end)::int as purchasers
from cohorts_user_orders
group by 1,2,3
)

select * from final
order by acquisition_month, months_since_acquisition, acquisition_device