with 
base as (
select 
    fe.event_id,
    fe.user_id,
    fe.ga_session_id,
    fe.event_timestamp,
    fei.item_id,
    fei.item_revenue_in_usd
from {{ ref('fct_events')}} fe
join {{ ref('fct_event_items')}} fei using (event_id)
where event_name = 'purchase'
),

purchase_revenue as (

select
    event_id, 
    sum(coalesce(item_revenue_in_usd,0)) as revenue,
    count(distinct item_id) as distinct_item_count
from base
group by event_id
),

purchase_events as (
  select distinct 
    event_id,
    user_id,
    ga_session_id,
    event_timestamp
  from base
)

select 
    pe.event_id AS purchase_id, 
    pe.user_id,
    pe.ga_session_id,
    CAST(pe.event_timestamp as date) AS purchase_timestamp,
    COALESCE(pr.revenue, 0) as purchase_revenue,
    pr.distinct_item_count
from purchase_events pe
join purchase_revenue pr 
using(event_id)
