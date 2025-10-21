with session_events as (
select

    user_id, 
    ga_session_id,
    min(source) as source,
    min(medium) as medium, 

    -- on-site events

    MAX(CASE WHEN event_name = 'view_item' THEN 1 ELSE 0 END) as did_view_item, 
    MAX(CASE WHEN event_name = 'add_to_cart' THEN 1 ELSE 0 END) as did_add_to_cart,
    MAX(CASE WHEN event_name = 'begin_checkout' THEN 1 ELSE 0 END) as did_begin_checkout,
    MAX(CASE WHEN event_name = 'add_shipping_info' THEN 1 ELSE 0 END) as did_add_shipping_info,
    MAX(CASE WHEN event_name = 'add_payment_info' THEN 1 ELSE 0 END) as did_add_payment_info,
    MAX(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) as did_purchase

from {{ ref('fct_events')}}

where event_name IN (
        'view_item', 'add_to_cart', 'begin_checkout', 
        'add_shipping_info', 'add_payment_info', 'purchase'
    )
    and event_timestamp <= current_date
    and event_timestamp >= current_date - interval '30' day

group by 1,2
),

-- cascade reach: a session "reaches" step N only if it reached all prior steps
cascade as (
  select
    user_id,
    ga_session_id,
    source,
    medium, 
    (did_view_item = 1) as reached_view_item,
    (did_view_item = 1 and did_add_to_cart = 1) as reached_add_to_cart,
    (did_view_item = 1 and did_add_to_cart = 1 and did_begin_checkout = 1) as reached_begin_checkout,
    (did_view_item = 1 and did_add_to_cart = 1 and did_begin_checkout = 1 and did_add_shipping_info = 1) as reached_add_shipping,
    (did_view_item = 1 and did_add_to_cart = 1 and did_begin_checkout = 1 and did_add_shipping_info = 1 and did_add_payment_info = 1) as reached_add_payment,
    (did_view_item = 1 and did_add_to_cart = 1 and did_begin_checkout = 1 and did_purchase = 1) as reached_purchase
  from session_events
)

select
    current_date::date as as_of_date,
    source::varchar as source,
    medium::varchar as medium, 
    sum(case when reached_view_item then 1 else 0 end)::int        as sessions_with_view_item,
    sum(case when reached_add_to_cart then 1 else 0 end)::int       as sessions_with_add_to_cart,
    sum(case when reached_begin_checkout then 1 else 0 end)::int    as sessions_with_begin_checkout,
    sum(case when reached_add_shipping then 1 else 0 end)::int      as sessions_with_add_shipping,
    sum(case when reached_add_payment then 1 else 0 end)::int       as sessions_with_add_payment,
    sum(case when reached_purchase then 1 else 0 end)::int          as sessions_with_purchase
from cascade
group by 1,2,3