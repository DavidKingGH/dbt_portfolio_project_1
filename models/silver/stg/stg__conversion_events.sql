with purchases as (
select 
    fe.event_id,
    fe.user_id,
    fe.ga_session_id,
    fe.event_timestamp
from {{ ref('fct_events')}} fe
where lower(event_name) = 'purchase'
),

purchase_revenue as (

select
    p.event_id, 
    sum(coalesce(item_revenue_in_usd,0)) as revenue,
    count(distinct item_id) as distinct_item_count
from purchases p
left join {{ ref('fct_event_items') }} fei
  on fei.event_id = p.event_id
group by 1
group by event_id
),

select 
    p.event_id as purchase_id, 
    p.user_id,
    p.ga_session_id,
    p.event_timestamp as purchase_timestamp,
    coalesce(ir.revenue, 0) as purchase_revenue,
    coalesce(ir.distinct_item_count,0) as distinct_item_count
from purchases p
left join item_rev ir
  on ir.event_id = p.event_id
