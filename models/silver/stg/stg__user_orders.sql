-- grain: one row per item per event_id

with purchase_events as (
  select
    event_id,
    user_id,
    ga_session_id,
    device_category,
    cast(event_date as timestamp) as order_date
  from {{ ref('fct_events') }}
  where event_name = 'purchase'
),

order_items as (
  select
    pe.event_id,
    
    -- aggregate order information 
    min(pe.order_date)  as order_date,
    min(pe.user_id) as user_id,
    min(pe.ga_session_id)  as ga_session_id,
    min(pe.device_category) as device_category,
    coalesce(sum(i.item_revenue_in_usd),0) as revenue,
    sum(coalesce(i.quantity,1)) as total_items,
    count(distinct i.item_id) as distinct_products
  from purchase_events pe
  left join {{ ref('fct_event_items') }} i
    on i.event_id = pe.event_id
  group by pe.event_id
  )

select * from order_items
